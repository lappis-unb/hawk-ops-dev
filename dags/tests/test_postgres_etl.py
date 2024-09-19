import pytest
from unittest import mock
from scripts.database_etl.postgres_etl import PostgresETL

# Teste para clone_tables_incremental
@mock.patch("scripts.database_etl.postgres_etl.PostgresHook")
@mock.patch("pandas.read_sql_query")
@mock.patch("scripts.database_etl.postgres_etl.PostgresETL.check_schema")
@mock.patch("scripts.database_etl.postgres_etl.PostgresETL.check_table")
@mock.patch("scripts.database_etl.postgres_etl.PostgresETL.process_chunk")
@mock.patch("scripts.database_etl.postgres_etl.PostgresETL.transform_data")
def test_clone_tables_incremental(
    mock_transform_data, mock_process_chunk, mock_check_table, mock_check_schema, mock_read_sql_query, mock_postgres_hook
):
    # Mockar o retorno do PostgresHook para as conexões
    mock_postgres_hook.return_value.get_sqlalchemy_engine.return_value = mock.MagicMock()

    # Criar a instância do PostgresETL
    etl = PostgresETL(
        source_conn_id="source_conn", 
        target_conn_id="target_conn",
        source_schema="source_schema",
        target_schema="target_schema",
        chunk_size=1000,  # Usamos um chunk_size menor para facilitar os testes
        max_threads=2,
        multithreading=False,
    )

    # Mockar o comportamento da conexão (mock da engine)
    etl.source_engine = mock_postgres_hook.return_value.get_sqlalchemy_engine()
    etl.target_engine = mock_postgres_hook.return_value.get_sqlalchemy_engine()

    # Mock para source_tables
    source_tables = [mock.Mock(name="source_table_1"), mock.Mock(name="source_table_2")]
    key_column = "id"
    target_table = "target_table"

    # Mock do resultado da função transform_data
    mock_transform_data.return_value = mock.MagicMock()

    # Mock para o retorno de read_sql_query com um DataFrame simulado
    mock_read_sql_query.return_value = mock.MagicMock()

    # Chamada do método clone_tables_incremental
    etl.clone_tables_incremental(source_tables, target_table, key_column)

    # Verificações:

    # Verificar se check_schema foi chamado com o schema correto
    mock_check_schema.assert_called_once_with(etl.target_schema, etl.target_engine)

    # Verificar se check_table foi chamado com a tabela de destino e DataFrame transformado
    mock_check_table.assert_called_once()

    # Verificar se process_chunk foi chamado (verificando o loop de chunks)
    assert mock_process_chunk.call_count > 0

    # Verificar se transform_data foi chamado
    assert mock_transform_data.call_count > 0




@mock.patch("scripts.database_etl.postgres_etl.PostgresHook")
def test_check_schema(mock_postgres_hook):
    # Mockar o engine de retorno
    mock_postgres_hook.return_value.get_sqlalchemy_engine.return_value = mock.MagicMock()
    engine = mock_postgres_hook.return_value.get_sqlalchemy_engine()

    # Criar a instância do PostgresETL
    etl = PostgresETL(
        source_conn_id="source_conn", 
        target_conn_id="target_conn",
        source_schema="source_schema",
        target_schema="target_schema"
    )

    # Mock do comportamento da conexão
    mock_conn = engine.connect.return_value.__enter__.return_value
    mock_result = mock_conn.execute.return_value

    # Definir que o resultado da consulta é vazio (schema não existe)
    mock_result.fetchone.return_value = None

    # Chamada do método check_schema
    etl.check_schema("target_schema", engine)

    # Verificar se o schema foi criado corretamente
    mock_conn.execute.assert_any_call("CREATE SCHEMA IF NOT EXISTS target_schema;")

    # Definir que o schema já existe
    mock_result.fetchone.return_value = True

    # Chamada do método novamente
    etl.check_schema("target_schema", engine)

    assert mock_conn.execute.call_count == 3  # O comando CREATE SCHEMA deve ser chamado apenas uma vez


# Teste individual de check_table
@mock.patch("scripts.database_etl.postgres_etl.inspect")
@mock.patch("scripts.database_etl.postgres_etl.PostgresHook")
def test_check_table(mock_postgres_hook, mock_inspect):
    # Mockar o engine de retorno
    mock_postgres_hook.return_value.get_sqlalchemy_engine.return_value = mock.MagicMock()
    engine = mock_postgres_hook.return_value.get_sqlalchemy_engine()

    # Criação de um mock para o inspector
    inspector = mock_inspect.return_value
    inspector.has_table.return_value = False  # Simular que a tabela não existe

    # Criar a instância do PostgresETL
    etl = PostgresETL(
        source_conn_id="source_conn", 
        target_conn_id="target_conn",
        source_schema="source_schema",
        target_schema="target_schema"
    )

    # Simular DataFrame de retorno
    df_mock = mock.MagicMock()

    # Chamar o método check_table
    etl.check_table("target_table", df_mock)

    # Verificar se a função de criar a tabela foi chamada
    df_mock.head.assert_called_once_with(0)
    df_mock.head().to_sql.assert_called_once_with(
        "target_table",
        etl.target_engine,
        schema="target_schema",
        if_exists="fail",
        index=False
    )

    # Verificar comportamento quando a tabela já existe
    inspector.has_table.return_value = True
    etl.check_table("target_table", df_mock)
    assert df_mock.head().to_sql.call_count == 1  # Não deve chamar to_sql novamente



# clone_tables_incremental
#    ├── check_schema
#    ├── check_table
#    ├── Loop para cada chunk:
#        ├── process_chunk
#            └── transform_data (chamada dentro de process_chunk)