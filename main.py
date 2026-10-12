from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
from app import Sanitizador, imprimir_resumo, montar_relatorio, parse_log
from app.meta import abrir_log, montar_meta
from app.resumo import montar_resumo_ia
from app.score import calcular_score
from app.validador import validar_relatorio

PASTA_SAIDA = Path("logs_results")


def ler_argumentos() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Analisa o stderr de uma execucao Spark no EMR.")
    origem = p.add_mutually_exclusive_group(required=True)
    origem.add_argument("--log", type=Path, help="Caminho do arquivo de log.")
    origem.add_argument("--env-var", help="Nome da variavel do .env que contem o caminho do log.")
    p.add_argument("--saida", type=Path,
                   help="Caminho do JSON de saida (padrao: logs_results/<nome_do_log>.json).")
    ia = p.add_argument_group("analise por IA (Etapa 4)")
    ia.add_argument("--ia", choices=["manual", "anthropic", "groq"],
                    help="manual: gera o prompt para colar no chat; anthropic/groq: chama a API.")
    ia.add_argument("--resumo", choices=["padrao", "compacto", "minimo"], default=None,
                    help="tamanho do resumo enviado a IA (padrao: 'compacto' com --ia groq, "
                         "'padrao' nos demais).")
    ia.add_argument("--resposta-ia", type=Path,
                    help="(modo manual) arquivo com a resposta do Claude, para validar e gravar.")
    ia.add_argument("--modelo", help="(anthropic/groq) id do modelo; padrao ANTHROPIC_MODEL / "
                                     "GROQ_MODEL do .env ou o definido em app/assistente.py.")
    ia.add_argument("--prompt", default=None, help="versao do prompt em prompts/ (padrao analise_v1).")
    ia.add_argument("--max-tokens-entrada", type=int, default=None,
                    help="limite de tokens de entrada do seu plano. Se o resumo passar, o perfil e "
                         "reduzido automaticamente (padrao -> compacto -> minimo). Padrao: 6000 "
                         "com --ia groq; sem limite nos demais. Tambem le IA_MAX_TOKENS_ENTRADA.")
    ia.add_argument("--forcar-ia", action="store_true",
                    help="envia para a IA mesmo com erro de consistencia (nao recomendado).")
    return p.parse_args()


def resolver_log(args: argparse.Namespace) -> Path:
    if args.log:
        return args.log
    from dotenv import find_dotenv, load_dotenv
    load_dotenv(find_dotenv(usecwd=True))
    valor = os.getenv(args.env_var)
    if not valor:
        raise SystemExit(f"Variavel {args.env_var} nao encontrada no .env.")
    return Path(valor)


def caminho_resumo(saida: Path) -> Path:
    """logs_results/X.json -> logs_results/X_resumo_ia.json"""
    return saida.with_name(f"{saida.stem}_resumo_ia.json")


def processar(caminho_log: Path, saida: Path | None = None,
              perfil_resumo: str = "padrao") -> tuple[dict, Path, Sanitizador]:
    """Le o log, monta o relatorio, valida, sanitiza e grava dois JSONs:

    * <saida>             relatorio completo (validacao, auditoria);
    * <saida>_resumo_ia   recorte compacto que sera enviado a IA.

    Devolve (relatorio_gravado, caminho_do_json, sanitizador).
    """
    caminho_log = Path(caminho_log)
    if not caminho_log.is_file():
        raise FileNotFoundError(f"Arquivo nao encontrado: {caminho_log}")
    if caminho_log.suffix.lower() == ".json":
        raise ValueError(f"'{caminho_log}' e um JSON gerado pelo pipeline, nao um log. "
                         "Passe em --log o arquivo .txt do stderr; o resumo e o prompt "
                         "sao gerados a partir dele.")

    saida = Path(saida) if saida else PASTA_SAIDA / f"{caminho_log.stem}.json"
    if saida.resolve() in (caminho_log.resolve(), caminho_resumo(saida).resolve()) \
            or caminho_resumo(saida).resolve() == caminho_log.resolve():
        raise ValueError(f"A saida '{saida}' sobrescreveria o proprio log de entrada.")
    saida.parent.mkdir(parents=True, exist_ok=True)

    sanitizador = Sanitizador()
    estatisticas_leitura: dict = {}
    texto, leitor = abrir_log(caminho_log)   # le e calcula o sha256 na mesma passada
    with texto:
        parsed = parse_log(texto, sanitizador, estatisticas_leitura)

    # "meta" vem primeiro no JSON: identifica o log, a versao e os limiares usados.
    relatorio = {"meta": montar_meta(caminho_log, leitor), **montar_relatorio(parsed)}
    relatorio["leitura_do_log"] = estatisticas_leitura
    relatorio["consistencia"] = validar_relatorio(relatorio)
    relatorio["score"] = calcular_score(relatorio)

    # Sanitizacao na fronteira: todo texto do relatorio passa pelo
    # sanitizador antes de ser gravado (rdd_desc, acao, call_site...).
    output = sanitizador.sanitizar(relatorio)

    with open(saida, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    # O resumo nasce do relatorio JA sanitizado e passa de novo pelo sanitizador
    # (mesmo mapa de pseudonimos) como segunda barreira.
    resumo = sanitizador.sanitizar(montar_resumo_ia(output, perfil_resumo))
    with open(caminho_resumo(saida), "w", encoding="utf-8") as f:
        json.dump(resumo, f, indent=2, ensure_ascii=False)

    return output, saida, sanitizador


def gerar_relatorio_legivel(saida: Path) -> Path:
    """Gera logs_results/X_relatorio.md com o JSON, o resumo e (se houver) a analise da IA."""
    import hashlib

    from app import assistente
    from app.relatorio_md import gerar_relatorio_md

    relatorio = json.loads(saida.read_text(encoding="utf-8"))
    resumo = json.loads(caminho_resumo(saida).read_text(encoding="utf-8"))
    analise = None
    arq_analise = caminhos_ia(saida)["analise"]
    if arq_analise.exists():
        analise = json.loads(arq_analise.read_text(encoding="utf-8"))
        enviado = hashlib.sha256(assistente.montar_mensagem(resumo).encode("utf-8")).hexdigest()
        if analise["meta_ia"].get("resumo_enviado_sha256") != enviado:
            print("[relatorio] A analise de IA existente foi feita sobre OUTRO resumo "
                  "(o log ou o codigo mudou); ela nao entra no relatorio. Rode a IA de novo.")
            analise = None
    destino = saida.with_name(f"{saida.stem}_relatorio.md")
    destino.write_text(gerar_relatorio_md(relatorio, resumo, analise), encoding="utf-8")
    return destino


def caminhos_ia(saida: Path) -> dict[str, Path]:
    base = saida.with_name(saida.stem)
    return {"prompt": Path(f"{base}_prompt_ia.md"),
            "analise": Path(f"{base}_analise_ia.json"),
            "correcao": Path(f"{base}_correcao_ia.md")}


def executar_ia(args: argparse.Namespace, saida: Path) -> None:
    from app import assistente

    versao = args.prompt or assistente.VERSAO_PROMPT_PADRAO
    resumo = json.loads(caminho_resumo(saida).read_text(encoding="utf-8"))
    arquivos = caminhos_ia(saida)

    if args.ia == "manual" and not args.resposta_ia:
        assistente.gravar_prompt_manual(resumo, arquivos["prompt"], versao)
        print(f"\n[IA manual] Prompt gerado em '{arquivos['prompt']}'.")
        print("  1. Cole o conteudo numa conversa nova do Claude.")
        print("  2. Salve a resposta (so o JSON) num arquivo, ex.: resposta.json")
        print(f"  3. Rode de novo com: --ia manual --resposta-ia resposta.json")
        return

    if args.ia == "manual":
        chamar = assistente.provedor_arquivo(args.resposta_ia)
        max_tentativas = 1          # no manual nao ha como pedir correcao automaticamente
    else:
        load_dotenv_se_existir()
        resumo = ajustar_resumo_ao_limite(args, saida, resumo, versao)
        fabrica = {"anthropic": assistente.provedor_anthropic, "groq": assistente.provedor_groq}
        chamar = fabrica[args.ia](args.modelo)
        max_tentativas = assistente.MAX_TENTATIVAS
        print(f"\n[IA] Enviando para {chamar.descricao} "
              f"(~{assistente.estimar_tokens(resumo, versao):,} tokens de entrada estimados)".replace(",", "."))

    try:
        resultado = assistente.analisar(resumo, chamar, versao, max_tentativas)
    except Exception as e:   # rede, chave invalida, cota... nao e erro de formato
        dica = ""
        if any(x in str(e) for x in ("413", "429", "rate_limit", "Request too large", "tokens per minute")):
            dica = ("\n  Dica: o pedido passou do limite de tokens do seu plano. Tente --resumo minimo, "
                    "espere 1 minuto, ou use um modelo com limite maior (--modelo).")
        raise SystemExit(f"\n[IA] Falha ao chamar o modelo ({chamar.descricao}): "
                         f"{type(e).__name__}: {e}{dica}")
    assistente.gravar_resultado(resultado, resumo, arquivos["analise"], chamar.descricao, versao)

    if resultado.analise:
        a = resultado.analise
        print(f"\n[IA] Analise valida em {resultado.tentativas} tentativa(s): "
              f"{a.classificacao.value}, {len(a.gargalos)} gargalo(s), "
              f"{len(a.recomendacoes)} recomendacao(oes) -> '{arquivos['analise']}'")
    else:
        print(f"\n[IA] Resposta INVALIDA apos {resultado.tentativas} tentativa(s). "
              f"Detalhes em '{arquivos['analise']}'.")
        if args.ia == "manual":
            arquivos["correcao"].write_text(
                assistente.mensagem_de_correcao(resultado.erros[-1]), encoding="utf-8")
            print(f"  Cole o texto de '{arquivos['correcao']}' na MESMA conversa, salve a nova "
                  "resposta e rode de novo.")


def ajustar_resumo_ao_limite(args: argparse.Namespace, saida: Path, resumo: dict, versao: str) -> dict:
    """Se o resumo passar do limite de tokens, regera com um perfil menor e grava por cima.

    O resumo gravado e sempre o que foi enviado; assim a conferencia das evidencias
    (e o hash usado no relatorio) continuam batendo.
    """
    from app import assistente
    from app.resumo import ORDEM_PERFIS, montar_resumo_ia

    limite = args.max_tokens_entrada or int(os.getenv("IA_MAX_TOKENS_ENTRADA", "0")) or \
        (6000 if args.ia == "groq" else 0)
    if not limite:
        return resumo

    perfil = resumo.get("fonte", {}).get("perfil_do_resumo", "padrao")
    relatorio = json.loads(saida.read_text(encoding="utf-8"))
    for proximo in ORDEM_PERFIS[ORDEM_PERFIS.index(perfil):]:
        candidato = resumo if proximo == perfil else montar_resumo_ia(relatorio, proximo)
        tokens = assistente.estimar_tokens(candidato, versao)
        if tokens <= limite:
            if proximo != perfil:
                print(f"\n[IA] Resumo reduzido de '{perfil}' para '{proximo}' para caber no limite "
                      f"({tokens} <= {limite} tokens estimados).")
                caminho_resumo(saida).write_text(json.dumps(candidato, indent=2, ensure_ascii=False),
                                                 encoding="utf-8")
            return candidato
    raise SystemExit(
        f"\n[IA] Nem o resumo 'minimo' cabe no limite ({tokens} > {limite} tokens estimados). "
        "Opcoes: usar --ia manual (sem limite por minuto), um plano/modelo com limite maior, "
        "ou aumentar --max-tokens-entrada se o seu limite real for maior.")


def load_dotenv_se_existir() -> None:
    try:
        from dotenv import find_dotenv, load_dotenv
        load_dotenv(find_dotenv(usecwd=True))
    except ImportError:
        pass


def imprimir_consistencia(consistencia: dict) -> None:
    status = "OK" if consistencia["ok"] else "COM ERROS"
    print(f"Consistencia: {status} ({consistencia['erros']} erro(s), "
          f"{consistencia['avisos']} aviso(s), {consistencia['infos']} info)")
    for a in consistencia["alertas"]:
        print(f"  [{a['severidade']:<5}] {a['codigo']}: {a['mensagem']}")


def main() -> None:
    args = ler_argumentos()
    try:
        perfil = args.resumo or ("compacto" if args.ia == "groq" else "padrao")
        output, saida, sanitizador = processar(resolver_log(args), args.saida, perfil)
    except (FileNotFoundError, ValueError) as e:
        raise SystemExit(f"Erro: {e}")

    meta = output["meta"]
    print(f"Log       : {meta['log_nome']} (sha256 {meta['log_sha256'][:12]}..., "
          f"parser {meta['parser_version']})")
    imprimir_resumo(output)
    sc = output["score"]
    if sc["score"] is None:
        print(f"Score     : nao calculado ({sc['motivo']})")
    else:
        print(f"Score     : {sc['score']}/100 ({sc['faixa']}) - "
              + ", ".join(f"{d['criterio']} -{d['pontos']:g}" for d in sc["descontos"]))
    imprimir_consistencia(output["consistencia"])
    resumo = caminho_resumo(saida)
    print(f"\nArquivo '{saida}' gerado com sucesso.")
    print(f"Resumo para IA: '{resumo}' ({resumo.stat().st_size / 1024:.1f} KB)")
    print(f"Hosts mascarados: {len(sanitizador.host_map)} | Paths mascarados: {len(sanitizador.path_map)}")

    if args.ia:
        if not output["consistencia"]["ok"] and not args.forcar_ia:
            raise SystemExit("\n[IA] Nao executada: a consistencia tem erro(s), entao o resumo nao e "
                             "confiavel. Corrija a extracao (ou use --forcar-ia).")
        executar_ia(args, saida)

    destino = gerar_relatorio_legivel(saida)
    print(f"\nRelatorio legivel: '{destino}'  (no VS Code: abra e tecle Ctrl+Shift+V)")


if __name__ == "__main__":
    main()