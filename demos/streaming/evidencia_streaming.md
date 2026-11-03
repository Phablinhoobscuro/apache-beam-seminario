# Evidência de validação — Demonstração Streaming

## Ambiente

```text
Python 3.14.7
Apache Beam 2.76.0
Windows PowerShell
Prism/DirectRunner
```

---

## Tarefa 1 — Fonte simulada

Comando:

```powershell
.\.venv\Scripts\python.exe src\streaming_vendas.py
```

Saída validada:

```text
Event Time:    1s | Produto: Notebook   | Categoria: Eletronicos | Quantidade: 2 | Preco: 3500.00
Event Time:    4s | Produto: Mouse      | Categoria: Perifericos | Quantidade: 5 | Preco: 100.00
Event Time:    7s | Produto: Monitor    | Categoria: Eletronicos | Quantidade: 1 | Preco: 1200.00
Event Time:   12s | Produto: Teclado    | Categoria: Perifericos | Quantidade: 2 | Preco: 150.00
Event Time:   15s | Produto: SSD 1TB    | Categoria: Eletronicos | Quantidade: 1 | Preco: 450.00
Event Time:   18s | Produto: Hub USB    | Categoria: Acessorios  | Quantidade: 3 | Preco: 120.00
```

Resultado:

```text
✓ TestStream funcional
✓ Event Time validado
✓ seis eventos processados
```

---

## Tarefa 2 — Windowing

Comando:

```powershell
.\.venv\Scripts\python.exe src\streaming_janelas.py
```

Saída validada:

```text
Janela [0s, 10s) | Categoria: Eletronicos | Faturamento: 8200.00
Janela [0s, 10s) | Categoria: Perifericos | Faturamento: 500.00
Janela [10s, 20s) | Categoria: Eletronicos | Faturamento: 450.00
Janela [10s, 20s) | Categoria: Perifericos | Faturamento: 300.00
Janela [10s, 20s) | Categoria: Acessorios  | Faturamento: 360.00
```

Resultado:

```text
✓ FixedWindows(10)
✓ agregação por categoria
✓ resultados conferidos
```

---

## Tarefa 3 — Watermark, Late Data, Triggers e Alertas

Comando:

```powershell
.\.venv\Scripts\python.exe src\streaming_triggers.py
```

Emissões ON_TIME:

```text
RESULTADO | Janela [0s, 10s) | Categoria: Eletronicos | Faturamento: 8200.00 | timing: ON_TIME
RESULTADO | Janela [0s, 10s) | Categoria: Perifericos | Faturamento: 500.00 | timing: ON_TIME

RESULTADO | Janela [10s, 20s) | Categoria: Eletronicos | Faturamento: 450.00 | timing: ON_TIME
RESULTADO | Janela [10s, 20s) | Categoria: Perifericos | Faturamento: 300.00 | timing: ON_TIME
RESULTADO | Janela [10s, 20s) | Categoria: Acessorios | Faturamento: 360.00 | timing: ON_TIME
```

Alerta:

```text
ALERTA | Janela [0s, 10s) | Categoria: Eletronicos
Faturamento: 8200.00
Limite: 5000.00
timing: ON_TIME
```

Late data:

```text
Produto: Webcam
Categoria: Perifericos
Event Time: 8s
Faturamento: 250.00
```

Emissão LATE:

```text
RESULTADO | Janela [0s, 10s) | Categoria: Perifericos | Faturamento: 750.00
Pane: timing: LATE, index: 1
```

Resultado:

```text
✓ watermark validado
✓ late data processado
✓ allowed lateness validado
✓ late trigger executado
✓ modo ACCUMULATING validado
✓ PaneInfo identificou LATE
✓ alerta validado
```

---

## Testes automatizados

Comando:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

Resultado validado:

```text
Ran 12 tests in 4.180s

OK
```

Testes Streaming aprovados:

```text
test_agregacoes_por_janela
test_alerta_dispara_acima_de_5000
test_evento_atrasado_pertence_primeira_janela
test_faturamento_da_venda
test_late_data_atualiza_perifericos_para_750
test_nao_dispara_alerta_abaixo_do_limite
```

Resumo:

```text
6 testes Batch
6 testes Streaming
12 testes no total
12 aprovados
0 falhas
0 erros
```

---

## Conclusão

A demonstração Streaming foi executada e validada com sucesso.

Foram comprovados:

```text
✓ TestStream
✓ Event Time
✓ FixedWindows
✓ CombinePerKey
✓ Watermark
✓ Late Data
✓ Allowed Lateness
✓ Triggers
✓ Accumulating Mode
✓ PaneInfo
✓ Alertas
✓ Testes automatizados
```

Status:

```text
FASE 4 — CONCLUÍDA
```
