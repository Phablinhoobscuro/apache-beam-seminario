"""Tarefa 3 - Agregações e indicadores da demonstração batch.

Recebe a PCollection de vendas VÁLIDAS (saída da Tarefa 2), onde cada
elemento é um dicionário no formato:

    {'data': '2026-10-01', 'produto': 'Notebook', 'categoria': 'Eletronicos',
     'quantidade': 2, 'preco': 3500.0}

e produz os indicadores de negócio. Os valores permanecem numéricos
(sem formatação monetária); a formatação é responsabilidade da Tarefa 4.
"""

import apache_beam as beam


def faturamento_da_venda(venda):
    """Regra de negócio: faturamento_da_venda = quantidade x preco."""
    return venda["quantidade"] * venda["preco"]


def faturamento_por_categoria(vendas_validas):
    """OBRIGATÓRIO. Retorna PCollection de pares (categoria, faturamento_total).

    Este é o contrato de saída consumido pela Tarefa 4. Exemplo:
        ('Eletronicos', 17400.0)
    A ordem dos elementos não é garantida (e não é requisito).
    """
    return (
        vendas_validas
        | "Calcular faturamento" >> beam.Map(
            lambda v: (v["categoria"], faturamento_da_venda(v))
        )
        | "Somar por categoria" >> beam.CombinePerKey(sum)
    )


def faturamento_total(vendas_validas):
    """RECOMENDADO. Retorna PCollection com 1 elemento: soma de todas as vendas."""
    return (
        vendas_validas
        | "Faturamento de cada venda" >> beam.Map(faturamento_da_venda)
        | "Somar faturamento total" >> beam.CombineGlobally(sum)
    )


def quantidade_total_unidades(vendas_validas):
    """OPCIONAL. Retorna PCollection com 1 elemento: total de unidades vendidas."""
    return (
        vendas_validas
        | "Extrair quantidade" >> beam.Map(lambda v: v["quantidade"])
        | "Somar unidades" >> beam.CombineGlobally(sum)
    )


def vendas_por_categoria(vendas_validas):
    """OPCIONAL. Retorna PCollection de pares (categoria, numero_de_vendas)."""
    return (
        vendas_validas
        | "Categoria de cada venda" >> beam.Map(lambda v: (v["categoria"], 1))
        | "Contar vendas por categoria" >> beam.CombinePerKey(sum)
    )
