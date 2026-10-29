import sys
import unittest
from decimal import Decimal
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
from leitura_validacao import CAMINHO_CSV, parse_venda, venda_valida

def carregar_vendas_validas():
    with open(CAMINHO_CSV, encoding="utf-8") as arquivo:
        next(arquivo)
        vendas = [parse_venda(l.strip()) for l in arquivo if l.strip()]
    return [v for v in vendas if venda_valida(v)]

VENDAS = carregar_vendas_validas()

class TestAgregacoes(unittest.TestCase):
    def test_somente_15_vendas_validas(self):
        self.assertEqual(len(VENDAS), 15)

    def test_faturamento_da_venda(self):
        venda = {"categoria": "Eletronicos", "quantidade": 2, "preco": Decimal("3500.00")}
        self.assertEqual(faturamento_da_venda(venda), Decimal("7000.00"))

    def test_faturamento_por_categoria(self):
        with TestPipeline() as p:
            r = faturamento_por_categoria(p | beam.Create(VENDAS))
            assert_that(r, equal_to([
                ("Eletronicos", Decimal("17400.00")),
                ("Perifericos", Decimal("3150.00")),
                ("Acessorios", Decimal("1620.00")),
            ]))

    def test_faturamento_total(self):
        with TestPipeline() as p:
            r = faturamento_total(p | beam.Create(VENDAS))
            assert_that(r, equal_to([Decimal("22170.00")]))

    def test_quantidade_total_unidades(self):
        with TestPipeline() as p:
            r = quantidade_total_unidades(p | beam.Create(VENDAS))
            assert_that(r, equal_to([46]))

    def test_vendas_por_categoria(self):
        with TestPipeline() as p:
            r = vendas_por_categoria(p | beam.Create(VENDAS))
            assert_that(r, equal_to([
                ("Eletronicos", 5),
                ("Perifericos", 6),
                ("Acessorios", 4),
            ]))

if __name__ == "__main__":
    unittest.main()
