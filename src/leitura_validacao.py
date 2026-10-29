"""Leitura, conversão e validação oficial do dataset batch."""
import csv
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
import apache_beam as beam

CAMINHO_CSV = Path(__file__).resolve().parent.parent / "data" / "vendas.csv"

def parse_venda(linha):
    try:
        campos = next(csv.reader([linha]))
        if len(campos) != 5:
            return None
        data, produto, categoria, quantidade, preco = [c.strip() for c in campos]
        return {
            "data": datetime.strptime(data, "%Y-%m-%d").date(),
            "produto": produto,
            "categoria": categoria,
            "quantidade": int(quantidade),
            "preco": Decimal(preco),
        }
    except (ValueError, InvalidOperation, StopIteration):
        return None

def venda_valida(venda):
    return bool(
        venda
        and venda["data"]
        and venda["produto"]
        and venda["categoria"]
        and venda["quantidade"] > 0
        and venda["preco"] > 0
    )

def ler_vendas_validas(pipeline, caminho_csv=CAMINHO_CSV):
    return (
        pipeline
        | "Ler CSV" >> beam.io.ReadFromText(str(caminho_csv), skip_header_lines=1)
        | "Converter campos" >> beam.Map(parse_venda)
        | "Validar vendas" >> beam.Filter(venda_valida)
    )
