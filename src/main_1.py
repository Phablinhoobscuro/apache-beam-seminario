import apache_beam as beam

vendas = [
    ("Notebook", 3500.00),
    ("Mouse", 80.00),
    ("Notebook", 3500.00),
    ("Teclado", 150.00),
    ("Mouse", 80.00),
]

with beam.Pipeline() as pipeline:
    resultado = (
        pipeline
        | "Criar vendas" >> beam.Create(vendas)
        | "Filtrar vendas validas" >> beam.Filter(lambda venda: venda[1] > 0)
        | "Agrupar por produto" >> beam.CombinePerKey(sum)
        | "Exibir resultado" >> beam.Map(print)
    )