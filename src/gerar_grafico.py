"""Geração do gráfico a partir da saída CSV produzida pelo Apache Beam."""

import csv
from pathlib import Path

import matplotlib.pyplot as plt


def formatar_brl(valor):
    """Formata um número como moeda brasileira."""
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def gerar_grafico_faturamento(caminho_csv, caminho_png):
    """Lê o CSV agregado e gera um gráfico de barras horizontal."""
    caminho_csv = Path(caminho_csv)
    caminho_png = Path(caminho_png)

    categorias = []
    faturamentos = []

    with caminho_csv.open("r", encoding="utf-8", newline="") as arquivo:
        leitor = csv.DictReader(arquivo)
        for linha in leitor:
            categorias.append(linha["categoria"])
            faturamentos.append(float(linha["faturamento"]))

    dados = sorted(
        zip(categorias, faturamentos),
        key=lambda item: item[1],
        reverse=True,
    )

    categorias = [item[0] for item in dados]
    faturamentos = [item[1] for item in dados]

    fig, ax = plt.subplots(figsize=(10, 5.5))
    barras = ax.barh(categorias, faturamentos)
    ax.invert_yaxis()

    ax.set_title("Faturamento por categoria")
    ax.set_xlabel("Faturamento (R$)")
    ax.set_ylabel("Categoria")

    limite = max(faturamentos) if faturamentos else 0

    for barra, valor in zip(barras, faturamentos):
        ax.text(
            barra.get_width() + limite * 0.015,
            barra.get_y() + barra.get_height() / 2,
            formatar_brl(valor),
            va="center",
        )

    ax.grid(axis="x", alpha=0.25)
    ax.set_axisbelow(True)

    if limite:
        ax.set_xlim(0, limite * 1.25)

    fig.tight_layout()

    caminho_png.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(caminho_png, dpi=160, bbox_inches="tight")

    print(f"Gráfico gerado: {caminho_png}")

    plt.show()


if __name__ == "__main__":
    raiz = Path(__file__).resolve().parent.parent
    gerar_grafico_faturamento(
        raiz / "resultados" / "faturamento_categoria.csv",
        raiz / "resultados" / "faturamento_categoria.png",
    )
