"""Conferência automática com os valores de referência do cenario.md.

Executar na raiz do projeto:
    python -m unittest discover -s tests -v
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from agregacoes import (
    faturamento_da_venda,
    faturamento_por_categoria,
    faturamento_total,
    quantidade_total_unidades,
    vendas_por_categoria,
)
from demo_agregacoes import CAMINHO_CSV, converter_linha, venda_valida
import csv


def carregar_vendas_validas():
    with open(CAMINHO_CSV, encoding="utf-8", newline="") as f:
        convertidas = [converter_linha(l) for l in csv.DictReader(f)]
    return [v for v in convertidas if venda_valida(v)]


VENDAS = carregar_vendas_validas()


class TestAgregacoes(unittest.TestCase):
    def test_somente_15_vendas_validas(self):
        self.assertEqual(len(VENDAS), 15)

    def test_faturamento_da_venda(self):
        notebook = {"categoria": "Eletronicos", "quantidade": 2, "preco": 3500.0}
        self.assertEqual(faturamento_da_venda(notebook), 7000.0)

    def test_faturamento_por_categoria(self):
        with TestPipeline() as p:
            resultado = faturamento_por_categoria(p | beam.Create(VENDAS))
            assert_that(
                resultado,
                equal_to(
                    [
                        ("Eletronicos", 17400.0),
                        ("Perifericos", 3150.0),
                        ("Acessorios", 1620.0),
                    ]
                ),
            )

    def test_faturamento_total(self):
        with TestPipeline() as p:
            resultado = faturamento_total(p | beam.Create(VENDAS))
            assert_that(resultado, equal_to([22170.0]))

    def test_quantidade_total_unidades(self):
        with TestPipeline() as p:
            resultado = quantidade_total_unidades(p | beam.Create(VENDAS))
            assert_that(resultado, equal_to([46]))

    def test_vendas_por_categoria(self):
        with TestPipeline() as p:
            resultado = vendas_por_categoria(p | beam.Create(VENDAS))
            assert_that(
                resultado,
                equal_to([("Eletronicos", 5), ("Perifericos", 6), ("Acessorios", 4)]),
            )


if __name__ == "__main__":
    unittest.main()
