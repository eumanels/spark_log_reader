"""Menu interativo (CLI) para rodar o pipeline sobre os logs da pasta logs/.

Uso (sempre a partir da RAIZ do repositorio):
    python menu.py                          # logs de ./logs, scripts de ./scripts
    python menu.py --pasta X --scripts Y    # outras pastas

Reaproveita as funcoes do main.py: o resultado e o mesmo de rodar
`python main.py --log <arquivo> [--script <script.py>] --ia <modo>`.
"""
from __future__ import annotations
import argparse
from pathlib import Path

from app import imprimir_resumo
from app.script_fonte import PASTA_SCRIPTS, listar_scripts, sugerir_script
from main import (
    caminho_resumo,
    caminhos_ia,
    executar_ia,
    gerar_relatorio_legivel,
    imprimir_consistencia,
    imprimir_script,
    processar,
)

PASTA_LOGS = Path("logs")
EXTENSOES = {".txt", ".log", ".stderr"}

OPCOES = {
    "1": ("Gerar o resumo/prompt para colar na IA (manual)", "manual"),
    "2": ("Gerar analise com Anthropic", "anthropic"),
    "3": ("Gerar analise com Groq", "groq"),
    "4": ("Sair", None),
}


# ------------------------------------------------------------------ utilidades
def _tamanho(n: float) -> str:
    for unidade in ("B", "KB", "MB"):
        if n < 1024:
            return f"{n:.0f} {unidade}" if unidade == "B" else f"{n:.1f} {unidade}"
        n /= 1024
    return f"{n:.1f} GB"


def _perguntar(texto: str) -> str:
    try:
        return input(texto).strip()
    except EOFError:          # Ctrl+D / entrada fechada: encerra como Ctrl+C
        raise KeyboardInterrupt from None


def listar_logs(pasta: Path) -> list[Path]:
    if not pasta.is_dir():
        return []
    return sorted((p for p in pasta.iterdir()
                   if p.is_file() and p.suffix.lower() in EXTENSOES),
                  key=lambda p: p.name.lower())


def escolher_log(logs: list[Path]) -> Path | None:
    print("\n=== Logs disponiveis ===")
    for i, p in enumerate(logs, start=1):
        print(f"  {i:>2} - {p.name}  ({_tamanho(p.stat().st_size)})")
    print("   0 - Sair")
    while True:
        r = _perguntar("\nEscolha o log: ")
        if r == "0":
            return None
        if r.isdigit() and 1 <= int(r) <= len(logs):
            return logs[int(r) - 1]
        print(f"  Opcao invalida. Digite um numero de 0 a {len(logs)}.")


def escolher_opcao(log: Path) -> str:
    print(f"\n=== {log.name} ===")
    for chave, (texto, _) in OPCOES.items():
        print(f"  {chave} - {texto}")
    while True:
        r = _perguntar("\nEscolha uma opcao: ")
        if r in OPCOES:
            return r
        print("  Opcao invalida. Digite 1, 2, 3 ou 4.")


def escolher_script(log: Path, pasta: Path) -> Path | None:
    """Pergunta qual script .py acompanha o log. Enter aceita a sugestao."""
    scripts = listar_scripts(pasta)
    if not scripts:
        print(f"\n(Nenhum script .py em '{pasta}/'; seguindo so com o resumo do log.)")
        return None
    sugerido = sugerir_script(scripts, log.stem)

    print(f"\n=== Enviar o script executado junto? (pasta {pasta}/) ===")
    for i, s in enumerate(scripts, start=1):
        marca = "  <- sugerido" if s == sugerido else ""
        print(f"  {i:>2} - {s.relative_to(pasta)}  ({_tamanho(s.stat().st_size)}){marca}")
    print("   0 - Sem script (so o resumo do log)")
    padrao = scripts.index(sugerido) + 1 if sugerido else 0
    while True:
        r = _perguntar(f"\nEscolha o script [Enter = {padrao}]: ") or str(padrao)
        if r == "0":
            return None
        if r.isdigit() and 1 <= int(r) <= len(scripts):
            return scripts[int(r) - 1]
        print(f"  Opcao invalida. Digite um numero de 0 a {len(scripts)}.")


def _args_ia(modo: str, resposta: Path | None = None) -> argparse.Namespace:
    """Mesmos campos que o main.py le em executar_ia/ajustar_resumo_ao_limite."""
    return argparse.Namespace(ia=modo, resposta_ia=resposta, modelo=None, prompt=None,
                              max_tokens_entrada=None, forcar_ia=False)


# ------------------------------------------------------------------- execucao
def rodar(log: Path, modo: str, script: Path | None) -> None:
    perfil = "compacto" if modo == "groq" else "padrao"
    extra = f" + script '{script.name}'" if script else ""
    print(f"\nProcessando '{log.name}'{extra} (resumo '{perfil}')...")
    output, saida, sanitizador = processar(log, None, perfil, script)

    imprimir_resumo(output)
    sc = output["score"]
    if sc["score"] is None:
        print(f"Score     : nao calculado ({sc['motivo']})")
    else:
        print(f"Score     : {sc['score']}/100 ({sc['faixa']})")
    imprimir_consistencia(output["consistencia"])
    print(f"Resumo para IA: '{caminho_resumo(saida)}'")
    print(f"Hosts mascarados: {len(sanitizador.host_map)} | Paths mascarados: {len(sanitizador.path_map)}")
    imprimir_script(caminho_resumo(saida))

    if not output["consistencia"]["ok"]:
        print("\n[IA] Nao executada: a consistencia tem erro(s), entao o resumo nao e confiavel.")
    else:
        executar_ia(_args_ia(modo), saida)
        if modo == "manual":
            _validar_resposta_manual(saida)

    destino = gerar_relatorio_legivel(saida)
    print(f"\nRelatorio legivel: '{destino}'")


def _validar_resposta_manual(saida: Path) -> None:
    """Passo opcional do modo manual: validar a resposta colada do chat."""
    padrao = caminhos_ia(saida)["analise"].with_name(f"{saida.stem}_resposta.json")
    print("\nDepois de colar o prompt no chat, salve a resposta (so o JSON) num arquivo.")
    r = _perguntar(f"Caminho da resposta para validar agora [Enter = pular, "
                   f"'p' = {padrao}]: ")
    if not r:
        return
    resposta = padrao if r.lower() == "p" else Path(r.strip('"'))
    if not resposta.is_file():
        print(f"  Arquivo '{resposta}' nao encontrado; pulei a validacao.")
        return
    executar_ia(_args_ia("manual", resposta), saida)


def main() -> None:
    p = argparse.ArgumentParser(description="Menu interativo do spark_log_reader.")
    p.add_argument("--pasta", type=Path, default=PASTA_LOGS,
                   help=f"pasta com os logs .txt/.log (padrao: {PASTA_LOGS}/)")
    p.add_argument("--scripts", type=Path, default=PASTA_SCRIPTS,
                   help=f"pasta com os scripts .py executados no EMR (padrao: {PASTA_SCRIPTS}/)")
    args = p.parse_args()

    while True:
        logs = listar_logs(args.pasta)
        if not logs:
            raise SystemExit(f"Nenhum log ({', '.join(sorted(EXTENSOES))}) encontrado em "
                             f"'{args.pasta}/'. Rode o menu a partir da raiz do repositorio.")

        log = escolher_log(logs)
        if log is None:
            break
        modo = OPCOES[escolher_opcao(log)][1]
        if modo is None:
            break
        script = escolher_script(log, args.scripts)

        try:
            rodar(log, modo, script)
        except SystemExit as e:        # executar_ia usa SystemExit para erros de API/limite
            if e.code not in (None, 0):
                print(e.code)
        except ImportError as e:
            print(f"\nErro: pacote '{e.name}' nao instalado. Rode: pip install -r requirements.txt")
        except Exception as e:          # chave ausente, arquivo invalido etc.: volta ao menu
            print(f"\nErro: {type(e).__name__}: {e}")
            if modo in ("anthropic", "groq"):
                print(f"  Confira {modo.upper()}_API_KEY no .env.")

        _perguntar("\nEnter para voltar a lista de logs...")

    print("Ate mais!")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrompido.")
