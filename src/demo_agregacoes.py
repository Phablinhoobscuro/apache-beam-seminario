"""Demonstração batch integrada."""
import apache_beam as beam
from agregacoes import (
    faturamento_por_categoria,
    faturamento_total,
    quantidade_total_unidades,
    vendas_por_categoria,
)
from leitura_validacao import ler_vendas_validas

def main():
    with beam.Pipeline() as pipeline:
        vendas_validas = ler_vendas_validas(pipeline)

        faturamento_por_categoria(vendas_validas) | "Exibir faturamento por categoria" >> beam.Map(
            lambda par: print(f"Faturamento | {par[0]}: {par[1]:.2f}")
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

if __name__ == "__main__":
    main()
