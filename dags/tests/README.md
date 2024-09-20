# Instruções para Rodar os Testes

Este guia explica como configurar o ambiente de testes e executar os testes unitários do projeto.

## Passos para Configuração e Execução

### 1. Criar o Virtual Environment

Primeiro, crie um ambiente virtual para isolar as dependências do projeto:

```bash
python -m venv venv-tests
```

### 2. Ativar o Virtual Environment

Ative o ambiente virtual com o seguinte comando:

```bash
source venv-tests/bin/activate
```

### 3. Instalar as Dependências de Testes

Com o ambiente virtual ativado, instale as dependências necessárias para rodar os testes:

```bash
pip install -r dags/tests/requirements.txt
```

### 4. Rodar os Testes

Execute os testes unitários com o `pytest`:

```bash
pytest -v dags/tests
```

### 5. Desativar o Virtual Environment

Após a execução dos testes, desative o ambiente virtual:

```bash
deactivate
```
