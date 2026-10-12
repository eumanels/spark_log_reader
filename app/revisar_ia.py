from __future__ import annotations
import argparse
import json
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ))

from app.modelos import AnaliseIA  # noqa: E402
from app.revisao import rascunho_markdown, revisar  # noqa: E402
from main import caminho_resumo, caminhos_ia  # noqa: E402


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--json", type=Path, required=True, help="JSON completo do log (logs_results/X.json)")
    p.add_argument("--saida", type=Path, default=RAIZ / "docs")
    a = p.parse_args()

    arq_analise = caminhos_ia(a.json)["analise"]
    if not arq_analise.exists():
        print(f"Nao achei {arq_analise}. Rode antes: python main.py --log ... --ia <modo>")
        return 1
    ia = json.loads(arq_analise.read_text(encoding="utf-8"))
    if not ia.get("analise"):
        print("A analise salva e invalida (nao passou no schema); corrija antes de revisar.")
        return 1
    resumo = json.loads(caminho_resumo(a.json).read_text(encoding="utf-8"))

    candidatos = revisar(AnaliseIA.model_validate(ia["analise"]), resumo)
    a.saida.mkdir(parents=True, exist_ok=True)
    destino = a.saida / f"revisao_{a.json.stem}.md"
    nome = resumo.get("fonte", {}).get("log_nome") or a.json.stem
    destino.write_text(rascunho_markdown(candidatos, nome, ia["meta_ia"]), encoding="utf-8")

    print(f"{len(candidatos)} candidato(s) a discordancia:")
    for c in candidatos:
        print(f"  [{c['tipo']:<16}] {c['onde']:<14} {c['detalhe'][:90]}")
    print(f"\nRascunho: {destino}")
    return 0


if __name__ == "__main__":
    sys.exit(main())