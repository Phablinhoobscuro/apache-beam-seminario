"""Fase 4 - Tarefa 1: fonte simulada da demonstração Streaming.

Esta primeira versão valida somente:
- TestStream como fonte simulada;
- PCollection não limitada;
- eventos de venda;
- timestamps de evento (Event Time);
- avanço controlado do watermark.

Windowing, agregações, late data e triggers serão adicionados nas
tarefas seguintes da Fase 4.

Executar na raiz do projeto:

    python src/streaming_vendas.py
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms.window import TimestampedValue


def criar_test_stream():
    """Cria a fonte simulada com os seis eventos definidos no cenário."""

    return (
        TestStream()
        # O watermark começa antes dos primeiros eventos.
        .advance_watermark_to(0)

        # Eventos que futuramente pertencerão à janela [0s, 10s).
        .add_elements(
            [
                TimestampedValue(
                    {
                        "produto": "Notebook",
                        "categoria": "Eletronicos",
                        "quantidade": 2,
                        "preco": 3500.00,
                    },
                    1,
                ),
                TimestampedValue(
                    {
                        "produto": "Mouse",
                        "categoria": "Perifericos",
                        "quantidade": 5,
                        "preco": 100.00,
                    },
                    4,
                ),
                TimestampedValue(
                    {
                        "produto": "Monitor",
                        "categoria": "Eletronicos",
                        "quantidade": 1,
                        "preco": 1200.00,
                    },
                    7,
                ),
            ]
        )

        # Indica progresso do fluxo para além dos 10 segundos.
        .advance_watermark_to(10)

        # Eventos que futuramente pertencerão à janela [10s, 20s).
        .add_elements(
            [
                TimestampedValue(
                    {
                        "produto": "Teclado",
                        "categoria": "Perifericos",
                        "quantidade": 2,
                        "preco": 150.00,
                    },
                    12,
                ),
                TimestampedValue(
                    {
                        "produto": "SSD 1TB",
                        "categoria": "Eletronicos",
                        "quantidade": 1,
                        "preco": 450.00,
                    },
                    15,
                ),
                TimestampedValue(
                    {
                        "produto": "Hub USB",
                        "categoria": "Acessorios",
                        "quantidade": 3,
                        "preco": 120.00,
                    },
                    18,
                ),
            ]
        )

        # Finaliza a fonte simulada depois dos eventos iniciais.
        .advance_watermark_to(20)
        .advance_watermark_to_infinity()
    )


class ExibirEvento(beam.DoFn):
    """Exibe cada venda junto com o Event Time atribuído pelo TestStream."""

    def process(self, venda, timestamp=beam.DoFn.TimestampParam):
        event_time = float(timestamp)

        print(
            f"Event Time: {event_time:>4.0f}s | "
            f"Produto: {venda['produto']:<10} | "
            f"Categoria: {venda['categoria']:<11} | "
            f"Quantidade: {venda['quantidade']} | "
            f"Preco: {venda['preco']:.2f}"
        )

        # Mantém o elemento disponível para futuras transformações.
        yield venda


def main():
    """Executa a primeira versão da demonstração Streaming."""

    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as pipeline:
        vendas = pipeline | "Fonte Streaming Simulada" >> criar_test_stream()

        vendas | "Exibir eventos e timestamps" >> beam.ParDo(ExibirEvento())


if __name__ == "__main__":
    main()
