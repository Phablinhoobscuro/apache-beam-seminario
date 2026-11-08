from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter


def brl(valor):
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def formatar_eixo(valor, _):
    return f"{valor:,.0f}".replace(",", ".")


def gerar():
    raiz = Path(__file__).resolve().parents[1]
    resultados = raiz / "resultados"
    resultados.mkdir(parents=True, exist_ok=True)

    caminho_png = resultados / "streaming_janelas.png"

    # Resultados demonstrados no projeto.
    rotulos = [
        "Eletrônicos",
        "Periféricos",
        "Eletrônicos",
        "Periféricos",
        "Acessórios",
        "Periféricos",
    ]

    valores = [8200, 500, 450, 300, 360, 750]
    limite_alerta = 5000

    # Figura mais larga para evitar cortes no último bloco.
    fig, ax = plt.subplots(figsize=(16, 7.5))

    posicoes = list(range(len(rotulos)))
    barras = ax.bar(posicoes, valores, width=0.68)

    # Título principal.
    ax.set_title(
        "Streaming — Processamento por janelas e atualização com Late Data",
        fontsize=18,
        pad=24,
        weight="bold",
    )

    # Subtítulo.
    ax.text(
        0.5,
        1.01,
        "Os eventos são processados conforme chegam e agrupados pelo Event Time.",
        transform=ax.transAxes,
        ha="center",
        va="bottom",
        fontsize=11,
    )

    ax.set_ylabel("Faturamento (R$)", labelpad=12)
    ax.yaxis.set_major_formatter(FuncFormatter(formatar_eixo))

    ax.set_xticks(posicoes)
    ax.set_xticklabels(rotulos, fontsize=10)

    ax.grid(axis="y", linestyle="--", alpha=0.25)
    ax.set_axisbelow(True)

    # Limite do alerta.
    ax.axhline(
        limite_alerta,
        linestyle="--",
        linewidth=1.5,
    )

    ax.text(
        5.35,
        limite_alerta + 120,
        "Limite do alerta: R$ 5.000",
        ha="right",
        va="bottom",
        fontsize=9,
    )

    # Separadores entre os blocos.
    ax.axvline(1.5, linewidth=1, alpha=0.25)
    ax.axvline(4.5, linewidth=1, alpha=0.25)

    maior_valor = max(valores)
    ax.set_ylim(0, maior_valor * 1.245)

    # Valores monetários sobre as barras.
    for barra, valor in zip(barras, valores):
        ax.text(
            barra.get_x() + barra.get_width() / 2,
            barra.get_height() + maior_valor * 0.018,
            brl(valor),
            ha="center",
            va="bottom",
            fontsize=9,
        )

    # Cabeçalhos dos blocos.
    ax.text(
        0.5,
        maior_valor * 1.205,
        "Janela [0s, 10s) — ON_TIME",
        ha="center",
        va="center",
        fontsize=11,
        weight="bold",
    )

    ax.text(
        3.0,
        maior_valor * 1.205,
        "Janela [10s, 20s) — ON_TIME",
        ha="center",
        va="center",
        fontsize=11,
        weight="bold",
    )

    ax.text(
        5.0,
        maior_valor * 1.205,
        "Late Data — janela [0s, 10s)",
        ha="center",
        va="center",
        fontsize=10,
        weight="bold",
    )

    # Destaque do alerta.
    ax.annotate(
        "ALERTA",
        xy=(0, 8200),
        xytext=(0.50, 8950),
        arrowprops=dict(arrowstyle="->"),
        ha="center",
        fontsize=11,
        weight="bold",
    )

    # Destaque do Late Data.
    ax.annotate(
        "+ R$ 250 de evento atrasado\nR$ 500 → R$ 750",
        xy=(5, 750),
        xytext=(4.45, 2350),
        arrowprops=dict(arrowstyle="->"),
        ha="center",
        fontsize=10,
    )

    # Indicação curta sob a última barra.
    ax.text(
        5,
        -0.075,
        "LATE",
        transform=ax.get_xaxis_transform(),
        ha="center",
        va="top",
        fontsize=10,
        weight="bold",
    )

    # Limpeza visual.
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    # Rodapé explicativo.
    fig.text(
        0.5,
        0.035,
        "O Streaming gera resultados por janela e pode revisar uma janela anterior quando chega Late Data.",
        ha="center",
        fontsize=10,
    )

    # Margens manuais para evitar corte no PNG.
    fig.subplots_adjust(
        left=0.07,
        right=0.965,
        top=0.835,
        bottom=0.17,
    )

    # Exportação.
    fig.savefig(
        caminho_png,
        dpi=200,
        bbox_inches="tight",
        pad_inches=0.25,
    )

    print(f"Gráfico Streaming gerado: {caminho_png}")

    plt.show()


if __name__ == "__main__":
    gerar()
