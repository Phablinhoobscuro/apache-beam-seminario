
import apache_beam as beam
import csv

from datetime import datetime
from decimal import Decimal, InvalidOperation


CAMINHO_CSV = "data/vendas.csv"


def parse_venda(linha):
    try:
        campos = next(csv.reader([linha]))

        if len(campos) != 5:
            return None

        data, produto, categoria, quantidade, preco = [
            campo.strip() for campo in campos
        ]

        # Conversão dos tipos
        data_convertida = datetime.strptime(
            data, "%Y-%m-%d"
        ).date()

        quantidade_convertida = int(quantidade)
        preco_convertido = Decimal(preco)

        venda = {
            "data": data_convertida,
            "produto": produto,
            "categoria": categoria,
            "quantidade": quantidade_convertida,
            "preco": preco_convertido
        }

        # Imprimir o valor e o tipo de cada variável
        print("\n=== TIPOS APÓS A CONVERSÃO ===")

        for campo, valor in venda.items():
            print(
                f"{campo}: valor = {valor!r} | "
                f"tipo = {type(valor).__name__}"
            )

        print(f"Tipo da variável venda: {type(venda).__name__}")

        return venda

    except (ValueError, InvalidOperation, StopIteration):
        print(f"Linha inválida: {linha}")
        return None


def venda_valida(venda):
    if venda is None:
        return False

    return (
        bool(venda["produto"])
        and bool(venda["categoria"])
        and venda["quantidade"] > 0
        and venda["preco"] > 0
    )


def exibir_venda(venda):
    print("\n=== VENDA VÁLIDA ===")
    print(venda)


with beam.Pipeline() as pipeline:

    vendas_validas = (
        pipeline
        | "Ler CSV" >> beam.io.ReadFromText(CAMINHO_CSV)
        | "Remover cabecalho" >> beam.Filter(
            lambda linha: not linha.lstrip("\ufeff").startswith("data,")
        )
        | "Converter campos" >> beam.Map(parse_venda)
        | "Validar vendas" >> beam.Filter(venda_valida)
    )

    vendas_validas | "Exibir vendas" >> beam.Map(exibir_venda)
