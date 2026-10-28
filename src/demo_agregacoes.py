"""Demonstração batch - Tarefa 3 (Agregações e indicadores).

ATENÇÃO: as funções `ler_e_validar` abaixo são uma leitura PROVISÓRIA,
criada apenas para esta tarefa rodar de ponta a ponta. Quando a Tarefa 2
(leitura e validação oficial) for integrada, basta trocar `ler_e_validar`
pela PCollection de vendas válidas dela. O arquivo `agregacoes.py` não
precisa mudar.

Como executar (na raiz do projeto, com a .venv ativada):
    python src/demo_agregacoes.py
"""

import csv
from pathlib import Path

import apache_beam as beam

from agregacoes import (
    faturamento_por_categoria,
    faturamento_total,
    quantidade_total_unidades,
    vendas_por_categoria,
)

CAMINHO_CSV = Path(__file__).resolve().parent.parent / "data" / "vendas.csv"


def converter_linha(linha):
    """Converte quantidade/preco para número. Retorna None se não converter."""
    try:
        return {
            "data": (linha["data"] or "").strip(),
            "produto": (linha["produto"] or "").strip(),
            "categoria": (linha["categoria"] or "").strip(),
            "quantidade": int(linha["quantidade"]),
            "preco": float(linha["preco"]),
        }
    except (ValueError, TypeError):
        return None


def venda_valida(v):
    """Regras do cenario.md: campos preenchidos, quantidade > 0 e preco > 0."""
    return bool(
        v
        and v["data"]
        and v["produto"]
        and v["categoria"]
        and v["quantidade"] > 0
        and v["preco"] > 0
    )


def ler_e_validar(pipeline):
    """Leitura provisória (substituída pela Tarefa 2)."""
    with open(CAMINHO_CSV, encoding="utf-8", newline="") as f:
        linhas = list(csv.DictReader(f))
    return (
        pipeline
        | "Ler linhas do CSV" >> beam.Create(linhas)
        | "Converter tipos" >> beam.Map(converter_linha)
        | "Manter somente vendas validas" >> beam.Filter(venda_valida)
    )


def main():
    with beam.Pipeline() as pipeline:
        vendas_validas = ler_e_validar(pipeline)

        (
            faturamento_por_categoria(vendas_validas)
            | "Exibir faturamento por categoria"
            >> beam.Map(lambda par: print(f"Faturamento | {par[0]}: {par[1]:.2f}"))
        )
        (
            faturamento_total(vendas_validas)
            | "Exibir faturamento total"
            >> beam.Map(lambda t: print(f"Faturamento TOTAL: {t:.2f}"))
        )
        (
            quantidade_total_unidades(vendas_validas)
            | "Exibir unidades"
            >> beam.Map(lambda q: print(f"Unidades vendidas (total): {q}"))
        )
        (
            vendas_por_categoria(vendas_validas)
            | "Exibir vendas por categoria"
            >> beam.Map(lambda par: print(f"Qtd. de vendas | {par[0]}: {par[1]}"))
        )


if __name__ == "__main__":
    main()
