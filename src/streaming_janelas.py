"""Fase 4 - Tarefa 2: Event Time, Windowing e agregação por categoria.

Reutiliza a fonte simulada criada na Tarefa 1 e adiciona:
- FixedWindows de 10 segundos;
- cálculo do faturamento de cada venda;
- agregação por categoria dentro de cada janela;
- exibição dos limites da janela e do faturamento resultante.

Executar na raiz do projeto:

    ./.venv/Scripts/python.exe src/streaming_janelas.py
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows

from streaming_vendas import criar_test_stream


TAMANHO_JANELA_SEGUNDOS = 10


def venda_para_faturamento(venda):
    """Converte uma venda em (categoria, faturamento)."""
    faturamento = venda["quantidade"] * venda["preco"]
    return venda["categoria"], faturamento


class ExibirResultadoJanela(beam.DoFn):
    """Exibe o faturamento de uma categoria junto com sua janela."""

    def process(self, elemento, window=beam.DoFn.WindowParam):
        categoria, faturamento = elemento

        inicio = int(float(window.start))
        fim = int(float(window.end))

        print(
            f"Janela [{inicio}s, {fim}s) | "
            f"Categoria: {categoria:<11} | "
            f"Faturamento: {faturamento:.2f}"
        )

        yield elemento


def main():
    """Executa a demonstração de janelas fixas da Tarefa 2."""

    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as pipeline:
        vendas = pipeline | "Fonte Streaming Simulada" >> criar_test_stream()

        vendas_por_janela = (
            vendas
            | "Aplicar janelas de 10 segundos"
            >> beam.WindowInto(FixedWindows(TAMANHO_JANELA_SEGUNDOS))
            | "Calcular faturamento por venda"
            >> beam.Map(venda_para_faturamento)
            | "Somar faturamento por categoria e janela"
            >> beam.CombinePerKey(sum)
        )

        vendas_por_janela | "Exibir resultados por janela" >> beam.ParDo(
            ExibirResultadoJanela()
        )


if __name__ == "__main__":
    main()
