from pathlib import Path
import csv
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

NOMES = {
    "Eletronicos": "Eletrônicos",
    "Perifericos": "Periféricos",
    "Acessorios": "Acessórios",
}

def brl(v):
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def eixo(v, _):
    return f"{v:,.0f}".replace(",", ".")

def gerar():
    raiz = Path(__file__).resolve().parents[1]
    resultados = raiz / "resultados"
    csv_path = resultados / "faturamento_categoria.csv"
    png_path = resultados / "faturamento_batch.png"

    dados = []
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        for linha in csv.DictReader(f):
            categoria = NOMES.get(linha["categoria"].strip(), linha["categoria"].strip())
            dados.append((categoria, float(linha["faturamento"])))

    dados.sort(key=lambda x: x[1], reverse=True)
    categorias = [x[0] for x in dados]
    valores = [x[1] for x in dados]

    fig, ax = plt.subplots(figsize=(11, 6.2))
    barras = ax.barh(categorias, valores)
    ax.invert_yaxis()

    ax.set_title("Batch — Faturamento final por categoria", fontsize=16, pad=18, weight="bold")
    ax.text(
        0.5, 1.01,
        "Todo o arquivo de vendas é processado antes da geração do resultado consolidado.",
        transform=ax.transAxes, ha="center", va="bottom", fontsize=10
    )

    ax.set_xlabel("Faturamento (R$)", labelpad=10)
    ax.xaxis.set_major_formatter(FuncFormatter(eixo))
    ax.grid(axis="x", linestyle="--", alpha=0.25)
    ax.set_axisbelow(True)

    maior = max(valores)
    ax.set_xlim(0, maior * 1.22)

    for barra, valor in zip(barras, valores):
        ax.text(
            barra.get_width() + maior * 0.015,
            barra.get_y() + barra.get_height() / 2,
            brl(valor),
            va="center", fontsize=11, weight="bold"
        )

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    fig.text(
        0.5, 0.02,
        "Resultado final após o processamento completo do conjunto de dados.",
        ha="center", fontsize=10
    )

    fig.tight_layout(rect=(0.04, 0.06, 0.98, 0.93))
    fig.savefig(png_path, dpi=180, bbox_inches="tight")
    print(f"Gráfico Batch gerado: {png_path}")
    plt.show()

if __name__ == "__main__":
    gerar()
