"""Demonstração batch integrada com saída CSV."""
from pathlib import Path

import apache_beam as beam

from agregacoes import (
    faturamento_por_categoria,
    faturamento_total,
    quantidade_total_unidades,
    vendas_por_categoria,
)
from leitura_validacao import ler_vendas_validas


PASTA_RESULTADOS = Path(__file__).resolve().parent.parent / "resultados"
PREFIXO_SAIDA_CSV = PASTA_RESULTADOS / "faturamento_categoria"


def formatar_linha_csv(par):
    """Converte (categoria, faturamento) para uma linha CSV."""
    categoria, faturamento = par
    return f"{categoria},{faturamento:.2f}"


def main():
    PASTA_RESULTADOS.mkdir(parents=True, exist_ok=True)

    with beam.Pipeline() as pipeline:
        vendas_validas = ler_vendas_validas(pipeline)

        # Calcula uma única vez e reutiliza a mesma PCollection
        # tanto no terminal quanto no arquivo CSV.
        faturamento_categoria = faturamento_por_categoria(vendas_validas)

        faturamento_categoria | "Exibir faturamento por categoria" >> beam.Map(
            lambda par: print(f"Faturamento | {par[0]}: {par[1]:.2f}")
        )

        (
            faturamento_categoria
            | "Formatar faturamento para CSV" >> beam.Map(formatar_linha_csv)
            | "Salvar faturamento em CSV"
            >> beam.io.WriteToText(
                str(PREFIXO_SAIDA_CSV),
                file_name_suffix=".csv",
                num_shards=1,
                shard_name_template="",
                header="categoria,faturamento",
            )
        )

        faturamento_total(vendas_validas) | "Exibir faturamento total" >> beam.Map(
            lambda total: print(f"Faturamento TOTAL: {total:.2f}")
        )

        quantidade_total_unidades(vendas_validas) | "Exibir unidades" >> beam.Map(
            lambda q: print(f"Unidades vendidas (total): {q}")
        )

        vendas_por_categoria(vendas_validas) | "Exibir vendas por categoria" >> beam.Map(
            lambda par: print(f"Qtd. de vendas | {par[0]}: {par[1]}")
        )

    arquivo_saida = PREFIXO_SAIDA_CSV.with_suffix(".csv")
    print(f"Arquivo CSV gerado: {arquivo_saida}")


if __name__ == "__main__":
    main()
