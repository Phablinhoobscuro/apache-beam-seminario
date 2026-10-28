# Evidência de conferência — Tarefa 3 (Agregações e indicadores)

## Como reproduzir

Na raiz do projeto, com a `.venv` ativada:

```powershell
python src/demo_agregacoes.py
python -m unittest discover -s tests -v
```

## Resultado da execução (`src/demo_agregacoes.py`)

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

A ordem das linhas pode variar entre execuções (não é requisito).

## Conferência com os valores de referência (`demos/batch/cenario.md`)

| Indicador | Esperado | Obtido | Confere |
|---|---|---|---|
| Faturamento Eletronicos | R$ 17.400,00 | 17400.00 | ✓ |
| Faturamento Perifericos | R$ 3.150,00 | 3150.00 | ✓ |
| Faturamento Acessorios | R$ 1.620,00 | 1620.00 | ✓ |
| Faturamento total | R$ 22.170,00 | 22170.00 | ✓ |
| Unidades totais | 46 | 46 | ✓ |
| Vendas válidas | 15 | 15 | ✓ |

Os 3 registros inválidos (Cabo USB com quantidade 0, Webcam com preço negativo
e linha com produto vazio) não entram nas agregações.

## Testes automatizados

`tests/test_agregacoes.py` usa `TestPipeline` e `assert_that` do próprio Beam
para comparar a saída de cada transformação com os valores de referência.
Resultado: 6 testes, todos aprovados.

## Contrato de saída para a Tarefa 4

`faturamento_por_categoria(vendas_validas)` retorna uma PCollection de pares:

```text
('Eletronicos', 17400.0)
('Perifericos', 3150.0)
('Acessorios', 1620.0)
```

Os valores são numéricos, sem formatação monetária.
