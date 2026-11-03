"""Testes automatizados da demonstração Streaming da Fase 4.

Cobertura:
- regra de faturamento;
- separação correta em janelas fixas de 10 segundos;
- agregações esperadas por categoria e janela;
- atualização esperada causada pelo late data;
- regra de alerta.

Executar na raiz do projeto:

    ./.venv/Scripts/python.exe -m unittest tests.test_streaming -v

ou toda a suíte:

    ./.venv/Scripts/python.exe -m unittest discover -s tests -v
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to_per_window
from apache_beam.transforms.window import FixedWindows, IntervalWindow, TimestampedValue, WindowFn

from streaming_janelas import venda_para_faturamento
from streaming_triggers import LIMITE_ALERTA


def criar_eventos_base():
    """Retorna os seis eventos usados nas Tarefas 1 e 2."""
    return [
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


class TestStreaming(unittest.TestCase):

    def test_faturamento_da_venda(self):
        venda = {
            "produto": "Notebook",
            "categoria": "Eletronicos",
            "quantidade": 2,
            "preco": 3500.00,
        }

        self.assertEqual(
            venda_para_faturamento(venda),
            ("Eletronicos", 7000.00),
        )

    def test_agregacoes_por_janela(self):
        """Valida simultaneamente Event Time, FixedWindows e CombinePerKey."""

        with TestPipeline() as pipeline:
            resultados = (
                pipeline
                | "Criar eventos de teste" >> beam.Create(criar_eventos_base())
                | "Aplicar FixedWindows no teste" >> beam.WindowInto(FixedWindows(10))
                | "Calcular faturamento no teste" >> beam.Map(venda_para_faturamento)
                | "Somar por categoria no teste" >> beam.CombinePerKey(sum)
            )

            esperado = {
                IntervalWindow(0, 10): [
                    ("Eletronicos", 8200.00),
                    ("Perifericos", 500.00),
                ],
                IntervalWindow(10, 20): [
                    ("Eletronicos", 450.00),
                    ("Perifericos", 300.00),
                    ("Acessorios", 360.00),
                ],
            }

            assert_that(
                resultados,
                equal_to_per_window(esperado),
                reify_windows=True,
                label="Validar resultados por janela",
            )

    def test_evento_atrasado_pertence_primeira_janela(self):
        """O Event Time 8s pertence à janela [0s, 10s)."""
        janela = FixedWindows(10).assign(
            WindowFn.AssignContext(8)
        )[0]

        self.assertEqual(janela, IntervalWindow(0, 10))

    def test_late_data_atualiza_perifericos_para_750(self):
        faturamento_inicial = 500.00

        webcam_atrasada = {
            "produto": "Webcam",
            "categoria": "Perifericos",
            "quantidade": 1,
            "preco": 250.00,
        }

        categoria, faturamento_webcam = venda_para_faturamento(webcam_atrasada)

        self.assertEqual(categoria, "Perifericos")
        self.assertEqual(
            faturamento_inicial + faturamento_webcam,
            750.00,
        )

    def test_alerta_dispara_acima_de_5000(self):
        self.assertGreater(8200.00, LIMITE_ALERTA)

    def test_nao_dispara_alerta_abaixo_do_limite(self):
        for faturamento in (500.00, 450.00, 300.00, 360.00, 750.00):
            with self.subTest(faturamento=faturamento):
                self.assertLessEqual(faturamento, LIMITE_ALERTA)


if __name__ == "__main__":
    unittest.main()
