import pytest
from unittest import mock
import pandas as pd
from sqlalchemy.exc import NoSuchTableError
from scripts.database_etl.postgres_etl import PostgresETL

# Teste individual de check_table
@pytest.mark.check_table
@mock.patch("scripts.database_etl.postgres_etl.inspect")
@mock.patch("scripts.database_etl.postgres_etl.PostgresHook")
def test_check_table(mock_postgres_hook, mock_inspect):
    """
    Should create the table if it doesn't exist and verify its schema matches the DataFrame.
    """
    
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
    assert df_mock.head().to_sql.call_count == 1, "Should call to_sql exactly once when table doesn't exist"

# Teste para garantir que a tabela já existe e o esquema é compatível
@pytest.mark.check_table
@mock.patch("scripts.database_etl.postgres_etl.inspect")
@mock.patch("scripts.database_etl.postgres_etl.PostgresHook")
def test_check_table_schema_matches(mock_postgres_hook, mock_inspect):
    """
    Should verify that the table schema matches the DataFrame column types.
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

    # Simular o retorno do inspector que indica que a tabela já existe
    inspector = mock_inspect.return_value
    inspector.has_table.return_value = True

    # Simular colunas da tabela existente com objetos que possuem `name` e `type`
    mock_column_id = mock.Mock(name="id")
    mock_column_id.name = "id"
    mock_column_id.type = "BIGINT"
    
    mock_column_name = mock.Mock(name="name")
    mock_column_name.name = "name"
    mock_column_name.type = "TEXT"
    
    # Mock para Table e MetaData
    with mock.patch("scripts.database_etl.postgres_etl.Table") as mock_table:
        mock_table.return_value.columns = [mock_column_id, mock_column_name]

        # Simular DataFrame com tipos correspondentes
        df_mock = pd.DataFrame({
            "id": pd.Series([1], dtype="int64"),
            "name": pd.Series(["test"], dtype="object")
        })

        # Chamar o método check_table
        etl.check_table("target_table", df_mock)

        # Verificar se o esquema é compatível
        assert mock_table.called, "Should check the existing table schema"
        assert mock_table.return_value.columns == [mock_column_id, mock_column_name], "Schema should match"


# Teste para verificar incompatibilidade de tipos
@pytest.mark.check_table
@mock.patch("scripts.database_etl.postgres_etl.inspect")
@mock.patch("scripts.database_etl.postgres_etl.PostgresHook")
def test_check_table_schema_incompatible(mock_postgres_hook, mock_inspect):
    """
    Should raise ValueError if the schema of the table and DataFrame are incompatible.
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

    # Simular o retorno do inspector que indica que a tabela já existe
    inspector = mock_inspect.return_value
    inspector.has_table.return_value = True

    # Simular colunas da tabela existente com objetos que possuem `name` e `type`
    mock_column_id = mock.Mock(name="id")
    mock_column_id.name = "id"
    mock_column_id.type = "BIGINT"
    
    mock_column_name = mock.Mock(name="name")
    mock_column_name.name = "name"
    mock_column_name.type = "TEXT"

    # Mock para Table e MetaData
    with mock.patch("scripts.database_etl.postgres_etl.Table") as mock_table:
        mock_table.return_value.columns = [mock_column_id, mock_column_name]

        # Simular DataFrame com tipos incompatíveis
        df_mock = pd.DataFrame({
            "id": pd.Series([1], dtype="int64"),
            "name": pd.Series(["test"], dtype="object"),
            "age": pd.Series([25], dtype="int64")
        })

        # Espera que um ValueError seja lançado devido à incompatibilidade de esquema
        with pytest.raises(ValueError, match="Incompatibilidade de esquema"):
            etl.check_table("target_table", df_mock)


# Teste para verificar incompatibilidade de tipos (coluna com tipo diferente do esperado)
@pytest.mark.check_table
@mock.patch("scripts.database_etl.postgres_etl.inspect")
@mock.patch("scripts.database_etl.postgres_etl.PostgresHook")
def test_check_table_schema_incompatible_column_type(mock_postgres_hook, mock_inspect):
    """
    Should raise ValueError if a column in the DataFrame has a different type than expected in the table schema.
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

    # Simular o retorno do inspector que indica que a tabela já existe
    inspector = mock_inspect.return_value
    inspector.has_table.return_value = True

    # Simular colunas da tabela existente com objetos que possuem `name` e `type`
    mock_column_id = mock.Mock(name="id")
    mock_column_id.name = "id"
    mock_column_id.type = "BIGINT"
    
    mock_column_name = mock.Mock(name="name")
    mock_column_name.name = "name"
    mock_column_name.type = "TEXT"

    # Mock para Table e MetaData
    with mock.patch("scripts.database_etl.postgres_etl.Table") as mock_table:
        mock_table.return_value.columns = [mock_column_id, mock_column_name]

        # Simular DataFrame onde a coluna 'name' tem tipo incompatível (int ao invés de TEXT)
        df_mock = pd.DataFrame({
            "id": pd.Series([1], dtype="int64"),
            "name": pd.Series([123], dtype="int64")  # Incompatível com TEXT
        })

        # Espera que um ValueError seja lançado devido à incompatibilidade de esquema
        with pytest.raises(ValueError, match="Incompatibilidade de esquema"):
            etl.check_table("target_table", df_mock)