# Seminário Apache Beam

Projeto acadêmico da disciplina **Projeto de Análise e Fluxo de Dados - OLAP e ETL**, com foco em **Apache Beam** e em pipelines de processamento **batch** e **streaming**.

## Ambiente utilizado

- Sistema operacional: Windows
- Terminal: Windows PowerShell
- Python: 3.14.7
- Apache Beam: 2.76.0
- Ambiente virtual Python: `venv` (`.venv`)
- Runner inicial planejado para os primeiros testes: DirectRunner

## Preparação do ambiente

### 1. Verificar a instalação do Python

No PowerShell, liste as instalações disponíveis:

```powershell
py -0p
```

Confirme a versão utilizada no projeto:

```powershell
py -3.14 --version
```

Resultado utilizado neste projeto:

```text
Python 3.14.7
```

### 2. Criar o ambiente virtual

Na raiz do projeto, execute:

```powershell
py -3.14 -m venv .venv
```

### 3. Ativar o ambiente virtual

No Windows PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
```

Quando a ativação for concluída, o terminal deverá exibir `(.venv)` no início da linha.

### 4. Confirmar o Python do ambiente virtual

```powershell
python --version
```

Resultado esperado:

```text
Python 3.14.7
```

### 5. Atualizar o pip

```powershell
python -m pip install --upgrade pip
```

Depois, confirme a instalação:

```powershell
pip --version
```

### 6. Instalar as dependências do projeto

Com o ambiente virtual ativado, execute:

```powershell
python -m pip install -r requirements.txt
```

O arquivo `requirements.txt` registra a dependência direta utilizada no projeto:

```text
apache-beam==2.76.0
```

### 7. Validar a instalação do Apache Beam

Confirme que o pacote pode ser importado e verifique sua versão:

```powershell
python -c "import apache_beam as beam; print(beam.__version__)"
```

Resultado esperado:

```text
2.76.0
```

Também é possível consultar os detalhes da instalação com:

```powershell
python -m pip show apache-beam
```

A instalação deve apontar para a pasta da `.venv`, confirmando que o Apache Beam está isolado no ambiente virtual do projeto.

## Dependências

O projeto utiliza dois arquivos para registrar dependências:

- `requirements.txt`: contém as dependências diretas definidas pelo projeto.
- `requirements-lock.txt`: registra todas as dependências e versões exatas instaladas no ambiente utilizado durante a configuração.

O arquivo de lock foi gerado com:

```powershell
pip freeze > requirements-lock.txt
```

## Arquivos ignorados pelo Git

O arquivo `.gitignore` contém:

```gitignore
.venv/
__pycache__/
*.pyc
```

Dessa forma, o ambiente virtual e arquivos temporários do Python não são versionados.

## Teste de replicabilidade

Para validar a preparação do ambiente, outro integrante do grupo deverá conseguir, em outro computador:

1. obter o projeto;
2. criar a própria `.venv`;
3. ativar o ambiente virtual;
4. instalar as dependências com `requirements.txt`;
5. importar o Apache Beam;
6. obter a versão `2.76.0` no teste de validação.

A Tarefa 1 da Fase 1 será considerada concluída após esse teste de replicabilidade.
