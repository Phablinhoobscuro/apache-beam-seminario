"""Tarefa 2 - Leitura, conversão e validação do CSV."""
import apache_beam as beam
from leitura_validacao import ler_vendas_validas

def exibir_venda(venda):
    print("\n=== VENDA VÁLIDA ===")
    print(venda)

def main():
    with beam.Pipeline() as pipeline:
        ler_vendas_validas(pipeline) | "Exibir vendas válidas" >> beam.Map(exibir_venda)

if __name__ == "__main__":
    main()
