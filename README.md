# Seminário Apache Beam

Projeto acadêmico da disciplina **Projeto de Análise e Fluxo de Dados - OLAP e ETL**, com foco em **Apache Beam** e em pipelines de processamento **batch** e **streaming**.

## Ambiente utilizado

- Sistema operacional: Windows
- Terminal: Windows PowerShell
- Python: 3.14.7
- Apache Beam: 2.76.0
- Ambiente virtual Python: `venv` (`.venv`)
- Runner inicial planejado para os primeiros testes: DirectRunner

## Estrutura do projeto

```text
apache-beam-seminario/
├── data/                  # Dados utilizados nas demonstrações
├── src/                   # Código-fonte e pipeline mínimo
├── demos/
│   ├── batch/             # Demonstração de processamento batch
│   └── streaming/         # Demonstração de processamento streaming
├── docs/
│   ├── pesquisa/          # Fundamentação teórica e materiais de pesquisa
│   └── arquitetura/       # Diagramas e documentação da arquitetura
├── slides/                # Arquivos da apresentação
├── README.md              # Documentação principal do projeto
├── requirements.txt       # Dependências diretas
├── requirements-lock.txt  # Versões exatas do ambiente validado
└── .gitignore             # Arquivos e pastas não versionados
```

Enquanto algumas pastas ainda não possuem conteúdo definitivo, é utilizado um arquivo `.gitkeep` para manter a estrutura versionada no GitHub. Esses arquivos podem ser removidos quando arquivos reais forem adicionados às respectivas pastas.

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

Dessa forma, o ambiente virtual e os arquivos temporários do Python não são versionados.

## Teste de replicabilidade

O teste de replicabilidade foi **concluído com sucesso em outro computador**.

O segundo ambiente conseguiu:

1. obter o projeto;
2. criar a própria `.venv`;
3. ativar o ambiente virtual;
4. instalar as dependências com `requirements.txt`;
5. importar o Apache Beam;
6. confirmar a versão `2.76.0` no teste de validação.

Com essa validação, a **Tarefa 1 da Fase 1 foi concluída**.

## Versionamento e colaboração

O projeto está versionado no GitHub e organizado para que os integrantes possam trabalhar nas próximas etapas sem compartilhar a pasta `.venv`.

As próximas contribuições deverão ser adicionadas às pastas correspondentes:

- `src/`: pipeline mínimo e demais códigos-fonte;
- `data/`: arquivos de entrada utilizados nas demonstrações;
- `demos/batch/`: demonstração de processamento batch;
- `demos/streaming/`: demonstração de processamento streaming;
- `docs/pesquisa/`: pesquisa e fundamentação teórica;
- `docs/arquitetura/`: materiais e diagramas de arquitetura;
- `slides/`: apresentação do seminário.
