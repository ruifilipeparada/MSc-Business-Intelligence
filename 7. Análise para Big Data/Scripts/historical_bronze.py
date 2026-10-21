import os
import time
import calendar
import requests


API_KEY = "6fc53d10d52e414f5b9bd3fc669bca5c"

BASE = "/home/u2250055/project_weatherstack/bronze" #alterar conforme o user que estiver a correr o código.

DISTRITOS = [
    "Aveiro", "Beja", "Braga", "Braganca", "Castelo Branco", "Coimbra",
    "Evora", "Faro", "Guarda", "Leiria", "Lisboa", "Portalegre",
    "Porto", "Santarem", "Setubal", "Viana do Castelo", "Vila Real", "Viseu",
]

# Intervalo de meses a extrair (inclusive).

START = (2025, 1)
END = (2026, 5)          # <-- mês 05-2026 incluído (último antes da entrega)

# hourly=1 captura o array horário, necessário para obter
# precipitação / humidade / vento ao nível diário no silver.

HOURLY = 1
INTERVAL = 1    #para obter os dados de todas as horas do dia         


def gerar_meses(start, end):
    """Gera (ano, mes) de start ate end, inclusive."""
    ano, mes = start
    while (ano, mes) <= end:
        yield ano, mes
        if mes == 12:
            ano, mes = ano + 1, 1
        else:
            mes += 1


def extrair(dist, ano, mes):
    ano_s = f"{ano:04d}"
    mes_s = f"{mes:02d}"
    ultimo_dia = calendar.monthrange(ano, mes)[1]

    start_date = f"{ano_s}-{mes_s}-01"
    end_date = f"{ano_s}-{mes_s}-{ultimo_dia:02d}"

    folder = os.path.join(BASE, f"ano={ano_s}", f"mes={mes_s}", f"distrito={dist}")
    os.makedirs(folder, exist_ok=True)

    params = {
        "access_key": API_KEY,
        "query": f"{dist},Portugal",
        "historical_date_start": start_date,
        "historical_date_end": end_date,
        "hourly": HOURLY,
    }
    if HOURLY:
        params["interval"] = INTERVAL

    r = requests.get("https://api.weatherstack.com/historical",
                     params=params, timeout=60)
    r.raise_for_status()
    data = r.json()

    # A weatherstack devolve HTTP 200 mesmo em erro -> validar o corpo.
    if not data.get("success", True) or "historical" not in data:
        erro = data.get("error", data)
        raise RuntimeError(f"Resposta invalida: {erro}")

    out_file = os.path.join(folder, f"{dist}_{ano_s}-{mes_s}.json")
    with open(out_file, "w", encoding="utf-8") as f:
        f.write(r.text)


def main():
    os.makedirs(BASE, exist_ok=True)
    falhas = []          #datas que não foram extraidas da API

    for ano, mes in gerar_meses(START, END):
        for dist in DISTRITOS:
            etiqueta = f"{dist} {ano:04d}-{mes:02d}"
            try:
                print(etiqueta)
                extrair(dist, ano, mes)
            except Exception as e:
                print(f"  FALHOU: {etiqueta} -> {e}")
                falhas.append(etiqueta)


    print("\nExtracao concluida.")
    if falhas:
        print(f"{len(falhas)} pedidos falharam:")
        for f in falhas:
            print(f"  - {f}")
    else:
        print("Sem falhas.")


if __name__ == "__main__":
    main()
