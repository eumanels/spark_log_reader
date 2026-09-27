"""Ponto de entrada: le o log indicado no .env, analisa e grava o JSON."""
from __future__ import annotations

import json
import os

from dotenv import find_dotenv, load_dotenv

from app import Sanitizador, imprimir_resumo, montar_relatorio, parse_log

VARIAVEL_ENV = "LOG_REP_APOLICE21"
ARQUIVO_SAIDA = "LOG_REP_APOLICE21.json"


def main() -> None:
    # Procura o .env a partir da pasta atual (mesmo comportamento do notebook).
    load_dotenv(find_dotenv(usecwd=True))

    path = os.getenv(VARIAVEL_ENV)
    if not path:
        raise ValueError(f"A variavel {VARIAVEL_ENV} nao foi encontrada no arquivo .env.")
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Arquivo nao encontrado: {path}")

    sanitizador = Sanitizador()
    with open(path, "r", encoding="utf-8") as f:
        parsed = parse_log(f, sanitizador)

    output = montar_relatorio(parsed)

    with open(ARQUIVO_SAIDA, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    imprimir_resumo(output)
    print(f"\nArquivo '{ARQUIVO_SAIDA}' gerado com sucesso.")
    print(f"Hosts mascarados: {len(sanitizador.host_map)} | Paths mascarados: {len(sanitizador.path_map)}")


if __name__ == "__main__":
    main()
