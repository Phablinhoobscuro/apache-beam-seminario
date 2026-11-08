# Seminário Apache Beam

Projeto acadêmico da disciplina **Projeto de Análise e Fluxo de Dados - OLAP e ETL**, com foco em **Apache Beam** e na construção de pipelines de processamento **Batch** e **Streaming**.

O projeto demonstra, de forma introdutória e prática, como o Apache Beam pode ser utilizado para ler, validar, transformar e agregar dados, além de trabalhar com conceitos de processamento contínuo como **Event Time**, **Windowing**, **Watermark**, **Triggers** e **Late Data**.

Para facilitar a apresentação dos resultados, o projeto utiliza também o **Matplotlib** como camada de visualização. O processamento continua sendo realizado pelo Apache Beam; o Matplotlib apenas representa graficamente a saída produzida pelos pipelines.

---

## Ambiente utilizado

- Sistema operacional: Windows
- Terminal: Windows PowerShell
- Python: 3.14.x
- Apache Beam: 2.76.0
- Matplotlib: 3.11.0
- Ambiente virtual Python: `venv` (`.venv`)
- Execução local: DirectRunner / PrismRunner

---

## Estrutura do projeto

```text
apache-beam-seminario/
├── data/
│   └── vendas.csv
│
├── src/
│   ├── leitura_validacao.py
│   ├── agregacoes.py
│   ├── demo_agregacoes.py
│   ├── gerar_grafico_batch.py
│   ├── streaming_vendas.py
│   ├── streaming_janelas.py
│   ├── streaming_triggers.py
│   └── gerar_grafico_streaming.py
│
├── tests/
│   ├── test_agregacoes.py
│   └── test_streaming.py
│
├── demos/
│   ├── batch/
│   └── streaming/
│
├── docs/
│   ├── pesquisa/
│   └── arquitetura/
│
├── resultados/
│   ├── faturamento_categoria.csv
│   ├── faturamento_batch.png
│   └── streaming_janelas.png
│
├── slides/
├── requirements.txt
├── requirements-lock.txt
├── README.md
└── .gitignore
```

A pasta `resultados/` é utilizada para armazenar arquivos gerados durante as demonstrações.

---

# Preparação do ambiente

## 1. Verificar a instalação do Python

No PowerShell:

```powershell
py -0p
```

Confirme que existe uma instalação compatível do Python 3.14:

```powershell
py -3.14 --version
```

Exemplo:

```text
Python 3.14.x
```

Caso o Python 3.14 não esteja instalado, em um computador com `winget` disponível pode ser utilizado:

```powershell
winget install -e --id Python.Python.3.14
```

Depois da instalação, feche e abra novamente o PowerShell.

---

## 2. Criar o ambiente virtual

Na raiz do projeto:

```powershell
py -3.14 -m venv .venv
```

Confirme que o Python da `.venv` foi criado:

```powershell
Test-Path .\.venv\Scripts\python.exe
```

Resultado esperado:

```text
True
```

> Não é obrigatório ativar a `.venv`. Os comandos deste README utilizam diretamente o executável localizado em `.\.venv\Scripts\python.exe`.

---

## 3. Instalar as dependências

Na raiz do projeto:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

As dependências diretas utilizadas atualmente são:

```text
apache-beam==2.76.0
matplotlib==3.11.0
```

Para confirmar as versões instaladas:

```powershell
.\.venv\Scripts\python.exe -c "import apache_beam as beam; import matplotlib; print('Beam:', beam.__version__); print('Matplotlib:', matplotlib.__version__)"
```

Resultado esperado:

```text
Beam: 2.76.0
Matplotlib: 3.11.0
```

---

# Demonstração Batch

## Objetivo

O processamento Batch utiliza um conjunto de dados finito já disponível em `data/vendas.csv`.

O fluxo executado é:

```text
vendas.csv
    ↓
leitura
    ↓
conversão dos campos
    ↓
validação
    ↓
remoção de registros inválidos
    ↓
agregações
    ↓
resultado consolidado
    ↓
faturamento_categoria.csv
```

O dataset contém registros válidos e inválidos propositalmente, permitindo demonstrar a etapa de validação do pipeline.

---

## Executar o Batch

```powershell
.\.venv\Scripts\python.exe src\demo_agregacoes.py
```

Resultados esperados:

```text
Qtd. de vendas | Eletronicos: 5
Qtd. de vendas | Perifericos: 6
Qtd. de vendas | Acessorios: 4

Faturamento | Eletronicos: 17400.00
Faturamento | Perifericos: 3150.00
Faturamento | Acessorios: 1620.00

Unidades vendidas (total): 46
Faturamento TOTAL: 22170.00
```

A ordem das linhas pode variar entre execuções.

Ao final, o pipeline gera:

```text
resultados\faturamento_categoria.csv
```

Conteúdo esperado:

```csv
categoria,faturamento
Eletronicos,17400.00
Perifericos,3150.00
Acessorios,1620.00
```

O `demo_agregacoes.py` executa apenas o processamento e a geração da saída CSV. A visualização foi mantida separada para deixar claro o papel de cada tecnologia.

---

# Visualização do Batch

O Apache Beam não fornece uma interface gráfica própria para apresentar os resultados processados.

Por esse motivo, o projeto utiliza o **Matplotlib** como camada de visualização.

O fluxo é:

```text
Apache Beam
    ↓
faturamento_categoria.csv
    ↓
Matplotlib
    ↓
faturamento_batch.png
```

Execute:

```powershell
.\.venv\Scripts\python.exe src\gerar_grafico_batch.py
```

Arquivo gerado:

```text
resultados\faturamento_batch.png
```

O gráfico apresenta o resultado consolidado do processamento Batch:

- Eletrônicos: R$ 17.400,00
- Periféricos: R$ 3.150,00
- Acessórios: R$ 1.620,00

---

# Demonstração Streaming

## Objetivo

Na demonstração Streaming, os dados são tratados como eventos que chegam ao longo do tempo.

Para permitir a execução local sem depender de Kafka, Pub/Sub ou outro sistema externo, o projeto utiliza `TestStream` para simular a chegada dos eventos.

São demonstrados os conceitos de:

- Event Time;
- Fixed Windows;
- Watermark;
- Triggers;
- Pane `ON_TIME`;
- Late Data;
- Pane `LATE`;
- modo de acumulação;
- alerta por limite de faturamento.

---

## Streaming por janelas

Execute:

```powershell
.\.venv\Scripts\python.exe src\streaming_janelas.py
```

Resultados esperados:

```text
Janela [0s, 10s) | Categoria: Eletronicos | Faturamento: 8200.00
Janela [0s, 10s) | Categoria: Perifericos | Faturamento: 500.00

Janela [10s, 20s) | Categoria: Eletronicos | Faturamento: 450.00
Janela [10s, 20s) | Categoria: Perifericos | Faturamento: 300.00
Janela [10s, 20s) | Categoria: Acessorios | Faturamento: 360.00
```

---

## Streaming com Watermark, Late Data, Triggers e alerta

Execute:

```powershell
.\.venv\Scripts\python.exe src\streaming_triggers.py
```

Entre os principais resultados estão:

```text
Janela [0s, 10s)
Eletronicos → 8200.00 → ON_TIME
Perifericos → 500.00 → ON_TIME
```

Como o limite configurado é de R$ 5.000,00, o faturamento de Eletrônicos gera:

```text
ALERTA
```

Posteriormente, chega um evento atrasado:

```text
Produto: Webcam
Categoria: Perifericos
Event Time: 8s
Faturamento: 250.00
```

Esse evento pertence à janela `[0s, 10s)`.

Como o pipeline permite Late Data e utiliza modo acumulativo, o resultado de Periféricos é atualizado:

```text
500.00 + 250.00 = 750.00
```

A nova pane é marcada como:

```text
LATE
```

---

# Visualização do Streaming

Para representar graficamente a diferença entre Batch e Streaming:

```powershell
.\.venv\Scripts\python.exe src\gerar_grafico_streaming.py
```

Arquivo gerado:

```text
resultados\streaming_janelas.png
```

O gráfico destaca:

- Janela `[0s, 10s)` — `ON_TIME`;
- Janela `[10s, 20s)` — `ON_TIME`;
- limite de alerta em R$ 5.000;
- alerta de Eletrônicos em R$ 8.200;
- Late Data;
- atualização de Periféricos de R$ 500 para R$ 750.

Assim, a diferença visual entre os dois modos de processamento fica clara:

```text
BATCH
conjunto completo
    ↓
processamento
    ↓
resultado final consolidado
```

```text
STREAMING
eventos contínuos
    ↓
processamento por janelas
    ↓
resultados ao longo do tempo
    ↓
possíveis atualizações com Late Data
```

---

# Testes automatizados

O projeto possui testes automatizados para os componentes Batch e Streaming.

Execute:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

A suíte atual possui:

```text
6 testes Batch
+
6 testes Streaming
=
12 testes
```

Resultado esperado:

```text
Ran 12 tests

OK
```

Durante a execução podem aparecer mensagens do PrismRunner. Elas fazem parte da execução local do Apache Beam e não significam falha caso a suíte termine com `OK`.

Entre os testes são validados:

- faturamento por venda;
- faturamento por categoria;
- faturamento total;
- quantidade total de unidades;
- quantidade de vendas válidas;
- vendas por categoria;
- Event Time;
- Fixed Windows;
- agregação por janela;
- regra de alerta;
- pertencimento de evento atrasado à janela;
- atualização de Periféricos para R$ 750 com Late Data.

---

# Roteiro rápido da demonstração prática

## 1. Processamento Batch

```powershell
.\.venv\Scripts\python.exe src\demo_agregacoes.py
```

Demonstrar:

- leitura do CSV;
- validação;
- agregações;
- geração do arquivo de saída.

---

## 2. Processamento Streaming

```powershell
.\.venv\Scripts\python.exe src\streaming_triggers.py
```

Demonstrar:

- `ON_TIME`;
- janelas;
- alerta;
- `LATE`;
- atualização de R$ 500 para R$ 750.

---

## 3. Visualização Batch

```powershell
.\.venv\Scripts\python.exe src\gerar_grafico_batch.py
```

---

## 4. Visualização Streaming

```powershell
.\.venv\Scripts\python.exe src\gerar_grafico_streaming.py
```

---

## 5. Testes

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

Resultado final esperado:

```text
Ran 12 tests
OK
```

---

# Apache Beam e Matplotlib

É importante separar o papel das tecnologias utilizadas:

### Apache Beam

Responsável por:

- leitura dos dados;
- validação;
- transformação;
- agregação;
- processamento Batch;
- processamento Streaming;
- Windowing;
- Event Time;
- Watermark;
- Triggers;
- Late Data.

### Matplotlib

Responsável apenas pela visualização dos resultados processados.

Portanto:

```text
Apache Beam = processamento
Matplotlib  = visualização
```

O Matplotlib não substitui o Apache Beam e não participa das regras de processamento dos dados.

---

# Dependências

O arquivo `requirements.txt` contém as dependências diretas do projeto:

```text
apache-beam==2.76.0
matplotlib==3.11.0
```

O `requirements-lock.txt` pode ser atualizado após a instalação das dependências:

```powershell
.\.venv\Scripts\python.exe -m pip freeze > requirements-lock.txt
```

---

# Arquivos ignorados pelo Git

O `.gitignore` deve manter pelo menos:

```gitignore
.venv/
__pycache__/
*.pyc
```

Dessa forma, o ambiente virtual e arquivos temporários do Python não são versionados.

---

# Replicabilidade

O projeto foi validado em mais de um computador.

Para executar em um novo ambiente:

```powershell
git clone <URL_DO_REPOSITORIO>
cd apache-beam-seminario

py -3.14 -m venv .venv

.\.venv\Scripts\python.exe -m pip install -r requirements.txt

.\.venv\Scripts\python.exe src\demo_agregacoes.py
```

Não é necessário compartilhar a pasta `.venv`, pois cada integrante pode recriar o ambiente a partir do `requirements.txt`.

---

# Resumo do projeto

```text
                         APACHE BEAM
                              │
                 ┌────────────┴────────────┐
                 │                         │
               BATCH                    STREAMING
                 │                         │
         conjunto completo            eventos contínuos
                 │                         │
            validação                  Event Time
                 │                      Windowing
          transformação                Watermark
                 │                      Triggers
           agregações                  Late Data
                 │                         │
                 └────────────┬────────────┘
                              │
                         resultados
                              │
                         Matplotlib
                              │
                    visualização gráfica
```

O projeto demonstra como o Apache Beam permite utilizar um mesmo modelo de programação para trabalhar com processamento Batch e Streaming, mantendo separada a camada de visualização dos resultados.
