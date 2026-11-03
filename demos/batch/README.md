# Demonstração Batch — Apache Beam

## 1. Objetivo

Esta demonstração apresenta um pipeline **batch** desenvolvido com Apache Beam para processar um conjunto finito de vendas armazenado em um arquivo CSV.

O fluxo implementado realiza:

```text
data/vendas.csv
      ↓
Leitura
      ↓
Conversão dos campos
      ↓
Validação dos registros
      ↓
Cálculo do faturamento
      ↓
Agrupamento por categoria
      ↓
Indicadores
      ↓
Resultado
```

O cenário representa, de forma simples, um processo semelhante a ETL:

```text
Extract
  ↓
ReadFromText

Transform
  ↓
parse_venda
venda_valida
agregações

Load / Output
  ↓
resultado exibido no terminal
```

## 2. Cenário de negócio

Uma empresa comercializa produtos de tecnologia e precisa consolidar seus registros de vendas para responder perguntas como:

- qual foi o faturamento por categoria;
- qual foi o faturamento total;
- quantas unidades foram vendidas;
- quantas vendas válidas ocorreram por categoria.

O arquivo de entrada também contém registros inválidos propositalmente, para demonstrar a etapa de limpeza e validação dos dados.

Mais detalhes estão em:

```text
demos/batch/cenario.md
```

## 3. Estrutura dos arquivos utilizados

```text
apache-beam-seminario/
│
├── data/
│   └── vendas.csv
│
├── src/
│   ├── leitura_validacao.py
│   ├── agregacoes.py
│   ├── main_2.py
│   └── demo_agregacoes.py
│
├── tests/
│   └── test_agregacoes.py
│
└── demos/
    └── batch/
        ├── README.md
        ├── cenario.md
        └── evidencia_agregacoes.md
```

### Responsabilidade de cada arquivo

- `data/vendas.csv`: dataset utilizado na demonstração.
- `src/leitura_validacao.py`: leitura, conversão de tipos e validação dos registros.
- `src/agregacoes.py`: funções responsáveis pelos indicadores e agregações.
- `src/main_2.py`: demonstração isolada da leitura e validação.
- `src/demo_agregacoes.py`: execução integrada da demonstração batch.
- `tests/test_agregacoes.py`: testes automatizados dos resultados.
- `demos/batch/cenario.md`: descrição do problema de negócio e valores de referência.
- `demos/batch/evidencia_agregacoes.md`: evidências dos resultados obtidos.

## 4. Dataset

O arquivo utilizado é:

```text
data/vendas.csv
```

Estrutura:

```csv
data,produto,categoria,quantidade,preco
2026-10-01,Notebook,Eletronicos,2,3500.00
2026-10-01,Mouse,Perifericos,5,100.00
...
```

| Campo | Tipo esperado | Descrição |
|---|---|---|
| `data` | data | Data da venda |
| `produto` | texto | Nome do produto |
| `categoria` | texto | Categoria do produto |
| `quantidade` | inteiro | Quantidade vendida |
| `preco` | decimal | Preço unitário |

## 5. Registros inválidos

O dataset possui três registros inválidos propositalmente:

```text
Cabo USB       → quantidade = 0
Webcam         → preço = -250.00
produto vazio  → campo obrigatório ausente
```

Esses registros devem ser descartados pela etapa de validação e não podem participar dos cálculos finais.

## 6. Regras de validação

Um registro é considerado válido quando:

1. a data está preenchida e possui formato válido;
2. o produto está preenchido;
3. a categoria está preenchida;
4. a quantidade é maior que zero;
5. o preço é maior que zero.

A implementação oficial dessas regras está em:

```text
src/leitura_validacao.py
```

## 7. Preparação do ambiente

A demonstração foi validada com:

```text
Python 3.14.7
Apache Beam 2.76.0
Windows
PowerShell
```

Na raiz do projeto:

```powershell
py -3.14 -m venv .venv
```

Ative o ambiente:

```powershell
.\.venv\Scripts\Activate.ps1
```

Se o PowerShell bloquear a execução do script, também é possível usar diretamente:

```powershell
.\.venv\Scripts\python.exe
```

Instale as dependências:

```powershell
python -m pip install -r requirements.txt
```

Confirme o Apache Beam:

```powershell
python -c "import apache_beam as beam; print(beam.__version__)"
```

Resultado esperado:

```text
2.76.0
```

## 8. Executar apenas a leitura e validação

```powershell
python src/main_2.py
```

Fluxo:

```text
data/vendas.csv
      ↓
ReadFromText
      ↓
parse_venda
      ↓
venda_valida
      ↓
vendas válidas
```

Os três registros inválidos não devem aparecer na saída válida.

## 9. Executar a demonstração Batch completa

Na raiz do projeto:

```powershell
python src/demo_agregacoes.py
```

Fluxo:

```text
vendas.csv
    ↓
ler_vendas_validas()
    ↓
PCollection de vendas válidas
    ↓
faturamento_por_categoria()
faturamento_total()
quantidade_total_unidades()
vendas_por_categoria()
    ↓
saída no terminal
```

## 10. Resultados esperados

A ordem das linhas pode variar entre execuções.

```text
Faturamento | Eletronicos: 17400.00
Faturamento | Perifericos: 3150.00
Faturamento | Acessorios: 1620.00

Faturamento TOTAL: 22170.00

Unidades vendidas (total): 46

Qtd. de vendas | Eletronicos: 5
Qtd. de vendas | Perifericos: 6
Qtd. de vendas | Acessorios: 4
```

| Indicador | Resultado esperado |
|---|---:|
| Faturamento Eletronicos | R$ 17.400,00 |
| Faturamento Perifericos | R$ 3.150,00 |
| Faturamento Acessorios | R$ 1.620,00 |
| Faturamento total | R$ 22.170,00 |
| Unidades vendidas | 46 |
| Vendas válidas | 15 |

## 11. Agregações implementadas

As funções principais estão em:

```text
src/agregacoes.py
```

### Faturamento de cada venda

```text
faturamento = quantidade × preco
```

### Faturamento por categoria

```text
(categoria, faturamento)
        ↓
CombinePerKey(sum)
```

### Faturamento total

```text
faturamento de cada venda
        ↓
CombineGlobally(sum)
```

### Quantidade total de unidades

```text
quantidade
    ↓
CombineGlobally(sum)
```

### Número de vendas por categoria

```text
(categoria, 1)
      ↓
CombinePerKey(sum)
```

## 12. Executar os testes automatizados

```powershell
python -m unittest discover -s tests -v
```

Os testes conferem:

1. se existem exatamente 15 vendas válidas;
2. o faturamento de uma venda individual;
3. o faturamento por categoria;
4. o faturamento total;
5. a quantidade total de unidades;
6. a quantidade de vendas por categoria.

Resultado esperado:

```text
Ran 6 tests
OK
```

Os testes utilizam recursos do Apache Beam como:

```text
TestPipeline
assert_that
equal_to
```

## 13. Valores utilizados nos testes

```text
Eletronicos → 17400.00
Perifericos → 3150.00
Acessorios  → 1620.00

Total → 22170.00
Unidades → 46
Vendas válidas → 15
```

## 14. O que a demonstração comprova

A execução demonstra que o Apache Beam consegue:

- ler um conjunto finito de dados;
- criar uma PCollection;
- converter os tipos dos registros;
- descartar entradas inválidas;
- aplicar regras de negócio;
- calcular indicadores;
- agrupar elementos por chave;
- realizar agregações;
- produzir resultados reproduzíveis;
- validar transformações com testes automatizados.

## 15. Relação com Batch

O dataset é finito e possui início e fim definidos.

```text
data/vendas.csv
      ↓
PCollection bounded
      ↓
processamento batch
      ↓
resultado
      ↓
pipeline termina
```

Esse comportamento diferencia esta demonstração da futura demo Streaming.

## 16. Evidências

Os resultados registrados durante o desenvolvimento podem ser consultados em:

```text
demos/batch/evidencia_agregacoes.md
```

Esse arquivo contém:

- comandos utilizados;
- resultados obtidos;
- comparação com os valores esperados;
- confirmação dos testes automatizados.

## 17. Roteiro rápido para apresentação

```text
1. Mostrar data/vendas.csv
        ↓
2. Mostrar os 3 registros inválidos
        ↓
3. Explicar leitura_validacao.py
        ↓
4. Executar demo_agregacoes.py
        ↓
5. Mostrar os indicadores
        ↓
6. Executar os testes
        ↓
7. Mostrar "Ran 6 tests / OK"
```

A ideia é demonstrar:

```text
Entrada → Validação → Transformação → Agregação → Resultado
```

## 18. Observação sobre execução

A ordem dos resultados produzidos pelo Apache Beam pode variar. Isso não representa erro.

O critério de validação são os **valores obtidos**, não a ordem em que aparecem no terminal.

## 19. Status

```text
✓ Dataset
✓ Cenário
✓ Leitura
✓ Conversão de tipos
✓ Validação
✓ Agregações
✓ Indicadores
✓ Testes automatizados
✓ Evidências
```

A demonstração representa a aplicação prática da **Fase 3 — Demonstração Batch** do projeto Apache Beam.
