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
    """
    Should correctly call methods related to schema checking, table checking,
    chunk processing and data transformation during incremental table cloning.
    """

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
    assert mock_check_schema.call_count == 1, "Should call check_schema exactly once"
    assert mock_check_table.call_count == 1, "Should call check_table exactly once"
    assert mock_process_chunk.call_count > 0, "Should call process_chunk at least once"
    assert mock_transform_data.call_count > 0, "Should call transform_data at least once"


@mock.patch("scripts.database_etl.postgres_etl.PostgresHook")
def test_check_schema(mock_postgres_hook):
    """
    Should create schema if it does not exist and not create it again if it already exists.
    """

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
    mock_conn.execute.assert_any_call(mock.ANY, {"schema_name": "target_schema"})
    mock_conn.execute.assert_any_call(mock.ANY)  # Não é mais comparável diretamente a uma string

    # Definir que o schema já existe
    mock_result.fetchone.return_value = True

    # Chamada do método novamente
    etl.check_schema("target_schema", engine)

    assert mock_conn.execute.call_count == 3, "Should execute 3 SQL commands (2 checks, 1 create)"
