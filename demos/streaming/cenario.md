# Demonstração Streaming — Cenário de negócio

## 1. Objetivo

A Fase 4 do projeto tem como objetivo demonstrar o processamento **streaming** com Apache Beam.

Enquanto a Fase 3 utilizou um arquivo CSV finito, processado em lote, a nova demonstração simulará vendas chegando continuamente ao pipeline ao longo do tempo.

O cenário foi mantido propositalmente próximo ao utilizado na demonstração Batch para permitir uma comparação direta entre os dois modelos.

Fluxo conceitual:

```text
Eventos de venda
      ↓
TestStream
      ↓
Event Time
      ↓
PCollection unbounded
      ↓
Fixed Windows
      ↓
Agregações
      ↓
Resultados por janela
```

---

## 2. Problema de negócio

Uma empresa comercializa produtos de tecnologia e recebe vendas continuamente de diferentes pontos de atendimento.

No processamento Batch, seria necessário aguardar a formação de um conjunto finito de registros para depois realizar a análise.

No cenário Streaming, a empresa deseja acompanhar as vendas conforme os eventos chegam ao sistema, permitindo obter indicadores em intervalos curtos de tempo.

A pergunta principal será:

> Qual é o faturamento de cada categoria em cada intervalo de tempo?

A demonstração também deverá permitir explicar situações em que um evento chega atrasado ao pipeline, mesmo possuindo um horário de ocorrência anterior.

---

## 3. Diferença em relação à Fase 3

### Batch

```text
data/vendas.csv
      ↓
todos os registros já existem
      ↓
processamento
      ↓
resultado final
      ↓
pipeline termina
```

### Streaming

```text
evento 1
   ↓
evento 2
   ↓
evento 3
   ↓
novos eventos continuam chegando
   ↓
resultados são calculados por intervalos de tempo
```

No Streaming, a coleção pode continuar recebendo elementos. Por isso, o processamento utilizará janelas temporais para dividir o fluxo em grupos finitos.

---

## 4. Fonte simulada

A fonte de dados da demonstração será criada com `TestStream`.

A escolha permite simular localmente:

- chegada contínua de eventos;
- timestamps associados às vendas;
- avanço do watermark;
- mudança entre janelas;
- chegada de eventos atrasados.

Dessa forma, a demonstração não depende de Kafka, Pub/Sub, RabbitMQ ou outro serviço externo.

---

## 5. Estrutura de cada evento

Cada venda simulada seguirá a estrutura:

```text
produto
categoria
quantidade
preco
```

Exemplo:

```python
{
    "produto": "Notebook",
    "categoria": "Eletronicos",
    "quantidade": 2,
    "preco": 3500.00
}
```

Além desses campos, cada elemento receberá um **timestamp de evento**.

O timestamp representa o momento em que a venda ocorreu, e não necessariamente o momento em que ela foi processada pelo pipeline.

---

## 6. Linha do tempo da demonstração

Para facilitar a visualização, os timestamps serão representados em segundos relativos ao início da simulação.

```text
0s                                                     20s
│-------------------------------------------------------│
   1s     4s      7s       12s      15s       18s
```

Os seis eventos principais serão:

| Event Time | Produto | Categoria | Quantidade | Preço unitário | Faturamento |
|---:|---|---|---:|---:|---:|
| 1 s | Notebook | Eletronicos | 2 | R$ 3.500,00 | R$ 7.000,00 |
| 4 s | Mouse | Perifericos | 5 | R$ 100,00 | R$ 500,00 |
| 7 s | Monitor | Eletronicos | 1 | R$ 1.200,00 | R$ 1.200,00 |
| 12 s | Teclado | Perifericos | 2 | R$ 150,00 | R$ 300,00 |
| 15 s | SSD 1TB | Eletronicos | 1 | R$ 450,00 | R$ 450,00 |
| 18 s | Hub USB | Acessorios | 3 | R$ 120,00 | R$ 360,00 |

---

## 7. Regra de faturamento

A mesma regra utilizada na Fase 3 será mantida:

```text
faturamento_da_venda = quantidade × preco
```

Exemplo:

```text
Notebook
quantidade = 2
preço = 3500.00

2 × 3500.00 = 7000.00
```

Depois, cada venda será transformada em:

```text
(categoria, faturamento)
```

para permitir a agregação por categoria.

---

## 8. Windowing

A primeira demonstração utilizará **janelas fixas de 10 segundos**.

Assim, a linha do tempo será dividida em:

```text
Janela 1
[0s, 10s)

Janela 2
[10s, 20s)
```

Os eventos serão associados à janela de acordo com o seu **Event Time**.

---

## 9. Janela 1 — 0 a 10 segundos

Eventos:

```text
1s  → Notebook → Eletronicos → 2 × 3500 = 7000
4s  → Mouse    → Perifericos → 5 × 100  = 500
7s  → Monitor  → Eletronicos → 1 × 1200 = 1200
```

Agregação esperada:

```text
Eletronicos
7000 + 1200 = 8200

Perifericos
500
```

Resultado esperado:

```text
JANELA [0s, 10s)

Eletronicos → R$ 8.200,00
Perifericos → R$ 500,00
```

---

## 10. Janela 2 — 10 a 20 segundos

Eventos:

```text
12s → Teclado → Perifericos → 2 × 150 = 300
15s → SSD 1TB → Eletronicos → 1 × 450 = 450
18s → Hub USB → Acessorios  → 3 × 120 = 360
```

Resultado esperado:

```text
JANELA [10s, 20s)

Perifericos → R$ 300,00
Eletronicos → R$ 450,00
Acessorios  → R$ 360,00
```

---

## 11. Resultado de referência inicial

Sem considerar dados atrasados, a demonstração deverá produzir:

```text
JANELA [0s, 10s)
Eletronicos → 8200.00
Perifericos → 500.00

JANELA [10s, 20s)
Eletronicos → 450.00
Perifericos → 300.00
Acessorios  → 360.00
```

Esses valores servirão como referência para os testes das próximas tarefas.

---

## 12. Watermark

Depois que os eventos de uma região temporal forem inseridos, o `TestStream` permitirá avançar o watermark.

Conceitualmente:

```text
eventos 1s, 4s e 7s
       ↓
watermark ultrapassa 10s
       ↓
janela [0s, 10s) pode emitir resultado
```

Depois:

```text
eventos 12s, 15s e 18s
       ↓
watermark ultrapassa 20s
       ↓
janela [10s, 20s) pode emitir resultado
```

O watermark será utilizado para demonstrar a noção de progresso temporal do fluxo.

---

## 13. Evento atrasado planejado

Na Tarefa 3 será introduzido propositalmente um evento atrasado.

Evento:

```text
produto: Webcam
categoria: Perifericos
quantidade: 1
preco: 250.00
event time: 8s
```

O evento possui timestamp de **8 segundos**, portanto pertence à primeira janela:

```text
[0s, 10s)
```

Entretanto, ele será inserido no `TestStream` somente depois que o watermark já tiver ultrapassado o fim dessa janela.

Representação:

```text
Event Time
8s

Processing / chegada simulada
depois do avanço do watermark para além de 10s
```

Isso permitirá demonstrar:

```text
EVENT TIME ≠ TEMPO DE CHEGADA / PROCESSAMENTO
```

---

## 14. Resultado esperado com o evento atrasado

Antes da chegada atrasada:

```text
JANELA [0s, 10s)

Eletronicos → 8200.00
Perifericos → 500.00
```

O evento atrasado acrescenta:

```text
Webcam
1 × 250 = 250
```

Assim, caso a configuração de trigger e lateness permita seu processamento, o resultado atualizado da categoria será:

```text
Perifericos
500 + 250 = 750
```

Resultado conceitual atualizado:

```text
JANELA [0s, 10s)

Eletronicos → 8200.00
Perifericos → 750.00
```

A implementação desse comportamento será realizada e validada na Tarefa 3.

---

## 15. Allowed Lateness planejado

Para a etapa de late data, será inicialmente considerada uma tolerância de atraso de:

```text
15 segundos
```

Esse valor foi escolhido apenas para fins didáticos, permitindo manter a janela disponível durante tempo suficiente para processar o evento atrasado planejado.

O valor poderá ser ajustado durante os testes caso o runner local exija adaptação.

---

## 16. Trigger planejado

A primeira versão do pipeline utilizará o comportamento mais simples possível para validar:

```text
TestStream
      ↓
FixedWindows(10)
      ↓
agregação
```

Somente depois dessa execução estar validada serão adicionados triggers específicos.

O objetivo da etapa posterior será demonstrar que os resultados podem ser emitidos:

```text
antes do fechamento da janela
        ou
quando o watermark passa pelo fim da janela
        ou
quando dados atrasados chegam
```

A implementação exata será definida após validar o comportamento no runner local.

---

## 17. Regra de alerta

Além dos resultados por janela, será utilizada uma regra simples para demonstrar monitoramento em Streaming:

> Emitir um alerta quando o faturamento de uma categoria ultrapassar R$ 5.000,00 dentro de uma janela.

Na primeira janela:

```text
Eletronicos → R$ 8.200,00
```

Portanto:

```text
ALERTA
Categoria: Eletronicos
Faturamento da janela: R$ 8.200,00
Limite: R$ 5.000,00
```

Na segunda janela, nenhuma categoria ultrapassa o limite.

---

## 18. Fluxo completo planejado

Ao final da Fase 4, a demonstração deverá representar:

```text
TestStream
      ↓
Eventos de venda com timestamps
      ↓
PCollection unbounded
      ↓
FixedWindows(10)
      ↓
(categoria, faturamento)
      ↓
CombinePerKey(sum)
      ↓
Resultados por janela
      ↓
Watermark
      ↓
Late Data
      ↓
Triggers
      ↓
Alertas
```

---

## 19. Divisão das próximas tarefas

### Tarefa 1 — Fonte simulada e cenário

Entregas:

```text
demos/streaming/cenario.md
src/streaming_vendas.py
```

Objetivo:

- definir os eventos;
- criar o `TestStream`;
- atribuir timestamps;
- confirmar que o pipeline recebe uma fonte simulada de Streaming.

### Tarefa 2 — Event Time e Windowing

Objetivo:

- aplicar `FixedWindows(10)`;
- calcular o faturamento de cada evento;
- agrupar os valores por categoria e janela;
- validar os resultados das duas janelas.

### Tarefa 3 — Watermark, atraso, triggers e alertas

Objetivo:

- controlar o avanço do watermark;
- inserir o evento atrasado;
- configurar lateness;
- testar trigger para dados atrasados;
- implementar a regra de alerta.

### Tarefa 4 — Saída, testes e documentação

Objetivo:

- integrar o pipeline completo;
- criar testes automatizados;
- registrar os resultados;
- criar evidências;
- produzir o README da demonstração Streaming.

---

## 20. Critérios de aceitação da Fase 4

A demonstração será considerada concluída quando for possível comprovar:

```text
✓ fonte de dados simulada com TestStream
✓ PCollection utilizada como fluxo não limitado
✓ eventos com Event Time
✓ janelas fixas funcionando
✓ faturamento agregado por categoria e janela
✓ resultados das duas janelas conferidos
✓ avanço de watermark demonstrado
✓ evento atrasado demonstrado
✓ comportamento de late data documentado
✓ trigger testado
✓ alerta de faturamento demonstrado
✓ testes automatizados
✓ documentação reproduzível
```

---

## 21. Resultado esperado da demonstração

A apresentação deverá permitir explicar visualmente a evolução:

```text
BATCH
dados completos
      ↓
processamento
      ↓
resultado único
```

para:

```text
STREAMING
eventos chegando ao longo do tempo
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
novos resultados
```

Dessa forma, a Fase 4 complementa a Fase 3 e demonstra dois dos principais modelos de processamento suportados pelo Apache Beam.
