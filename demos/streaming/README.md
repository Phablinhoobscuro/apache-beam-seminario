# Demonstração Streaming — Apache Beam

## 1. Objetivo

Esta pasta documenta a demonstração de processamento **Streaming** desenvolvida na Fase 4 do projeto Apache Beam.

A demo evolui a partir da Fase 3 (Batch) e demonstra, de forma prática:

- `TestStream`;
- Event Time;
- `FixedWindows(10)`;
- agregações por categoria;
- watermark;
- late data;
- `allowed_lateness`;
- triggers;
- `AccumulationMode.ACCUMULATING`;
- `PaneInfo`;
- regra de alerta;
- testes automatizados.

---

## 2. Ambiente validado

- Sistema operacional: Windows
- Terminal: PowerShell
- Python: 3.14.7
- Apache Beam: 2.76.0
- Runner local: Prism/DirectRunner
- Ambiente virtual: `.venv`

Para validar o ambiente:

```powershell
.\.venv\Scripts\python.exe --version
.\.venv\Scripts\python.exe -c "import apache_beam as beam; print(beam.__version__)"
```

Resultados esperados:

```text
Python 3.14.7
2.76.0
```

---

## 3. Estrutura da demonstração

```text
demos/streaming/
├── cenario.md
├── README.md
└── evidencia_streaming.md

src/
├── streaming_vendas.py
├── streaming_janelas.py
└── streaming_triggers.py

tests/
└── test_streaming.py
```

---

## 4. Etapas da Fase 4

### Tarefa 1 — Fonte simulada e cenário

Arquivo principal:

```text
src/streaming_vendas.py
```

Responsabilidades:

- criar o `TestStream`;
- inserir seis eventos com timestamps;
- executar em modo streaming;
- exibir os eventos com Event Time.

Execução:

```powershell
.\.venv\Scripts\python.exe src\streaming_vendas.py
```

Eventos esperados:

```text
1s   Notebook
4s   Mouse
7s   Monitor
12s  Teclado
15s  SSD 1TB
18s  Hub USB
```

---

### Tarefa 2 — Event Time e Windowing

Arquivo principal:

```text
src/streaming_janelas.py
```

Responsabilidades:

- aplicar `FixedWindows(10)`;
- calcular faturamento por venda;
- agrupar por categoria;
- somar o faturamento por janela.

Execução:

```powershell
.\.venv\Scripts\python.exe src\streaming_janelas.py
```

Resultados esperados:

```text
Janela [0s, 10s)
Eletronicos → 8200.00
Perifericos → 500.00

Janela [10s, 20s)
Eletronicos → 450.00
Perifericos → 300.00
Acessorios  → 360.00
```

---

### Tarefa 3 — Watermark, Late Data, Triggers e Alertas

Arquivo principal:

```text
src/streaming_triggers.py
```

Responsabilidades:

- controlar o watermark;
- aceitar evento atrasado;
- usar `allowed_lateness = 15s`;
- usar `AfterWatermark(late=AfterCount(1))`;
- usar `AccumulationMode.ACCUMULATING`;
- exibir `PaneInfo`;
- emitir alerta quando o faturamento ultrapassar R$ 5.000,00.

Execução:

```powershell
.\.venv\Scripts\python.exe src\streaming_triggers.py
```

Comportamento validado:

```text
Janela [0s, 10s)
Eletronicos → 8200.00 | ON_TIME
Perifericos → 500.00  | ON_TIME

Janela [10s, 20s)
Eletronicos → 450.00  | ON_TIME
Perifericos → 300.00  | ON_TIME
Acessorios  → 360.00  | ON_TIME
```

Evento atrasado:

```text
Webcam
Categoria: Perifericos
Event Time: 8s
Faturamento: 250.00
```

Nova emissão:

```text
Janela [0s, 10s)
Perifericos → 750.00 | LATE
```

O valor foi atualizado em modo acumulativo:

```text
500.00 + 250.00 = 750.00
```

Alerta validado:

```text
ALERTA
Janela [0s, 10s)
Categoria: Eletronicos
Faturamento: 8200.00
Limite: 5000.00
```

---

## 5. Tarefa 4 — Saída, testes e documentação

Arquivo de testes:

```text
tests/test_streaming.py
```

A suíte Streaming possui 6 testes, cobrindo:

- cálculo de faturamento;
- agregações por janela;
- Event Time;
- pertencimento do evento atrasado à primeira janela;
- atualização de `Perifericos` para 750.00;
- regra de alerta.

Executar somente os testes Streaming:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -p "test_streaming.py" -v
```

Executar toda a suíte:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

Resultado validado:

```text
Ran 12 tests in 4.180s

OK
```

A suíte completa contém:

```text
6 testes Batch
+
6 testes Streaming
=
12 testes
```

---

## 6. Fluxo completo da demonstração

```text
TestStream
      ↓
Eventos com Event Time
      ↓
PCollection em modo streaming
      ↓
FixedWindows(10)
      ↓
(categoria, faturamento)
      ↓
CombinePerKey(sum)
      ↓
resultado ON_TIME
      ↓
Watermark
      ↓
Late Data
      ↓
Late Trigger
      ↓
resultado LATE
      ↓
alerta
```

---

## 7. Comparação Batch × Streaming

### Batch

```text
arquivo finito
      ↓
todos os dados já existem
      ↓
processamento
      ↓
resultado final
```

### Streaming

```text
eventos continuam chegando
      ↓
Event Time
      ↓
Windowing
      ↓
resultados por janela
      ↓
watermark
      ↓
dados atrasados
      ↓
novas emissões
```

---

## 8. Status da Fase 4

```text
Tarefa 1 — Fonte simulada e cenário
✓ concluída

Tarefa 2 — Event Time e Windowing
✓ concluída

Tarefa 3 — Watermark, Late Data, Triggers e Alertas
✓ concluída

Tarefa 4 — Saída, testes e documentação
✓ concluída
```

A demonstração Streaming está pronta para integração na apresentação final do seminário.
