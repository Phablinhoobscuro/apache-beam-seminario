"""Fase 4 - Tarefa 3: Watermark, Late Data, Triggers e Alertas.

Demonstra:
- avanço do watermark;
- evento atrasado (late data);
- allowed lateness de 15 segundos;
- trigger ON_TIME + late firing;
- modo de acumulação ACCUMULATING;
- alerta para faturamento acima de R$ 5.000,00 por categoria/janela.

Executar na raiz do projeto:

    ./.venv/Scripts/python.exe src/streaming_triggers.py
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms.trigger import (
    AccumulationMode,
    AfterCount,
    AfterWatermark,
)
from apache_beam.transforms.window import FixedWindows, TimestampedValue


TAMANHO_JANELA_SEGUNDOS = 10
ATRASO_PERMITIDO_SEGUNDOS = 15
LIMITE_ALERTA = 5000.00


def venda(produto, categoria, quantidade, preco):
    """Cria um evento de venda."""
    return {
        "produto": produto,
        "categoria": categoria,
        "quantidade": quantidade,
        "preco": preco,
    }


def criar_test_stream_com_atraso():
    """Cria o fluxo com um evento atrasado pertencente à primeira janela."""

    return (
        TestStream()
        .advance_watermark_to(0)

        # Janela [0s, 10s)
        .add_elements(
            [
                TimestampedValue(
                    venda("Notebook", "Eletronicos", 2, 3500.00),
                    1,
                ),
                TimestampedValue(
                    venda("Mouse", "Perifericos", 5, 100.00),
                    4,
                ),
                TimestampedValue(
                    venda("Monitor", "Eletronicos", 1, 1200.00),
                    7,
                ),
            ]
        )

        # O watermark alcança o fim da primeira janela.
        # A janela [0s, 10s) pode emitir o resultado ON_TIME.
        .advance_watermark_to(10)

        # Janela [10s, 20s)
        .add_elements(
            [
                TimestampedValue(
                    venda("Teclado", "Perifericos", 2, 150.00),
                    12,
                ),
                TimestampedValue(
                    venda("SSD 1TB", "Eletronicos", 1, 450.00),
                    15,
                ),
                TimestampedValue(
                    venda("Hub USB", "Acessorios", 3, 120.00),
                    18,
                ),
            ]
        )

        # Fecha a segunda janela pelo watermark.
        .advance_watermark_to(20)

        # LATE DATA:
        # a Webcam chega agora, quando o watermark já está em 20s,
        # mas seu Event Time é 8s. Portanto, ela pertence à janela [0s, 10s).
        # Como allowed_lateness = 15s, a primeira janela continua apta
        # a aceitar esse evento até o watermark atingir 25s.
        .add_elements(
            [
                TimestampedValue(
                    venda("Webcam", "Perifericos", 1, 250.00),
                    8,
                )
            ]
        )

        # Leva o watermark até o limite de retenção da primeira janela.
        .advance_watermark_to(25)
        .advance_watermark_to_infinity()
    )


def venda_para_faturamento(venda_evento):
    """Transforma a venda em (categoria, faturamento)."""
    faturamento = venda_evento["quantidade"] * venda_evento["preco"]
    return venda_evento["categoria"], faturamento


class ExibirResultadoComPane(beam.DoFn):
    """Exibe resultados e metadados de pane para evidenciar os triggers."""

    def process(
        self,
        elemento,
        window=beam.DoFn.WindowParam,
        pane_info=beam.DoFn.PaneInfoParam,
    ):
        categoria, faturamento = elemento

        inicio = int(float(window.start))
        fim = int(float(window.end))

        print(
            f"RESULTADO | Janela [{inicio}s, {fim}s) | "
            f"Categoria: {categoria:<11} | "
            f"Faturamento: {faturamento:.2f} | "
            f"Pane: {pane_info}"
        )

        yield elemento


class ExibirAlerta(beam.DoFn):
    """Emite alertas de faturamento acima do limite configurado."""

    def process(
        self,
        elemento,
        window=beam.DoFn.WindowParam,
        pane_info=beam.DoFn.PaneInfoParam,
    ):
        categoria, faturamento = elemento

        if faturamento > LIMITE_ALERTA:
            inicio = int(float(window.start))
            fim = int(float(window.end))

            print(
                f"ALERTA    | Janela [{inicio}s, {fim}s) | "
                f"Categoria: {categoria} | "
                f"Faturamento: {faturamento:.2f} | "
                f"Limite: {LIMITE_ALERTA:.2f} | "
                f"Pane: {pane_info}"
            )

        yield elemento


def main():
    """Executa a demonstração completa da Tarefa 3."""

    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as pipeline:
        vendas_streaming = (
            pipeline
            | "Fonte com evento atrasado" >> criar_test_stream_com_atraso()
        )

        resultados = (
            vendas_streaming
            | "Janelas fixas de 10 segundos"
            >> beam.WindowInto(
                FixedWindows(TAMANHO_JANELA_SEGUNDOS),
                trigger=AfterWatermark(
                    late=AfterCount(1)
                ),
                accumulation_mode=AccumulationMode.ACCUMULATING,
                allowed_lateness=ATRASO_PERMITIDO_SEGUNDOS,
            )
            | "Calcular faturamento por venda"
            >> beam.Map(venda_para_faturamento)
            | "Somar por categoria e janela"
            >> beam.CombinePerKey(sum)
        )

        exibidos = (
            resultados
            | "Exibir resultados e panes"
            >> beam.ParDo(ExibirResultadoComPane())
        )

        exibidos | "Verificar alertas" >> beam.ParDo(ExibirAlerta())


if __name__ == "__main__":
    main()
