from __future__ import annotations
from .modelos import AnaliseIA
from .verificacao import conferir_analise, conferir_classificacao

NOME_FAIXA = {"boa": "🟢 boa", "atencao": "🟡 atenção", "critica": "🔴 crítica"}

SELO = {"confere": "✅ confere", "diverge": "⚠️ diverge", "inexistente": "❌ não existe no resumo"}
NOME_CLASSE = {"saudavel": "🟢 Saudável", "degradada": "🟡 Degradada",
               "com_falha": "🔴 Com falha", "inconclusiva": "⚪ Inconclusiva"}
ICONE_SEV = {"ALTA": "🔴 ALTA", "MEDIA": "🟠 MÉDIA", "BAIXA": "🟡 BAIXA", "INFO": "⚪ INFO"}
ICONE_PRIO = {"Alta": "🔴 Alta", "Media": "🟠 Média", "Baixa": "🟡 Baixa"}


def _c(valor) -> str:
    """Texto seguro para celula de tabela Markdown."""
    if valor is None:
        return "—"
    return str(valor).replace("|", "\\|").replace("\n", " ")


def _tabela(cabecalho: list[str], linhas: list[list]) -> str:
    out = ["| " + " | ".join(cabecalho) + " |", "|" + "---|" * len(cabecalho)]
    out += ["| " + " | ".join(_c(v) for v in linha) + " |" for linha in linhas]
    return "\n".join(out)


def _duracao(s) -> str:
    if s is None:
        return "—"
    m, seg = divmod(int(round(s)), 60)
    return f"{m} min {seg:02d} s" if m else f"{seg} s"


def gerar_relatorio_md(relatorio: dict, resumo: dict, analise_ia: dict | None = None) -> str:
    meta = relatorio.get("meta", {})
    ctx = resumo.get("contexto", {})
    ex = resumo.get("execucao", {})
    fa = resumo.get("falhas", {})
    cons = resumo.get("qualidade_dos_dados", {})

    analise = None
    conferencia = None
    classif = None
    if analise_ia and analise_ia.get("analise"):
        analise = AnaliseIA.model_validate(analise_ia["analise"])
        conferencia = conferir_analise(analise, resumo)
        classif = conferir_classificacao(analise, resumo)
    sc = relatorio.get("score") or {}
    texto_score = (f"**{sc['score']}/100** ({NOME_FAIXA.get(sc['faixa'], sc['faixa'])})"
                   if sc.get("score") is not None else f"não calculado — {sc.get('motivo', 'sem dados')}")

    md: list[str] = []
    md.append(f"# Relatório de execução — {ctx.get('app_name') or meta.get('log_nome')}")
    md.append(f"_Log `{meta.get('log_nome')}` · gerado em {meta.get('gerado_em')} · "
              f"parser {meta.get('parser_version')}"
              + (f" · prompt {analise_ia['meta_ia']['prompt_versao']} ({analise_ia['meta_ia']['provedor']})"
                 if analise else "") + "_")

    # ------------------------------------------------------------- visao geral
    md.append("## Visão geral")
    classe = NOME_CLASSE.get(analise.classificacao.value) if analise else "Análise por IA não executada"
    md.append(_tabela(["Indicador", "Valor"], [
        ["Score de qualidade (determinístico)", texto_score],
        ["Classificação (IA)", classe],
        ["Duração total", _duracao(ex.get("duracao_total_s"))],
        ["Código de saída", ex.get("exit_code")],
        ["Spark", ctx.get("spark_version")],
        ["Executores com tasks / máximo", f"{ex.get('executores_com_tasks')} / {ex.get('max_executors')}"],
        ["Ocupação estimada do cluster", f"{ex.get('ocupacao_pct')} %"],
        ["Executores perdidos", fa.get("executor_lost_total")],
        ["Falhas de stage", fa.get("stage_failures_total")],
        ["Retentativas de task", fa.get("task_retries_total")],
        ["Consistência dos dados", "OK" if cons.get("consistencia_ok") else "COM ERROS"],
    ]))
    if analise:
        md.append(f"**Por que essa classificação:** {analise.justificativa_classificacao}")
        if not classif["confere"]:
            md.append(f"⚠️ **A classificação da IA diverge do critério do prompt:** esperado "
                      f"`{classif['esperada']}`, a IA respondeu `{classif['obtida']}`.")
        t = conferencia["totais"]
        md.append(f"**Conferência das evidências da IA:** {t['confere']} conferem, {t['diverge']} divergem, "
                  f"{t['inexistente']} não existem no resumo ({conferencia['pct_confere']}% conferem).")

    # ------------------------------------------------------------------ script
    script = resumo.get("script")
    if script:
        s = script["sanitizacao"]
        md.append("## Script analisado")
        md.append(f"`{script['nome']}` · {script['total_linhas']} linhas no arquivo · enviado em modo "
                  f"**{script['modo']}** ({script['linhas_enviadas']} linhas) · sha256 "
                  f"`{script['sha256'][:12]}…`")
        md.append(f"_Antes do envio: {s['comentarios_removidos']} comentário(s) removido(s), "
                  f"{s['emails']} e-mail(s) e {s['segredos']} credencial(is) mascarados; paths e hosts "
                  f"com os mesmos pseudônimos do log._")
        if script["vinculos_log"]:
            md.append("**Linhas do script citadas pelo próprio log**")
            md.append(_tabela(["Linha", "Código", "Ação no log", "Jobs", "Duração dos jobs (s)", "Stages"],
                              [[v["linha"], f"`{script['linhas'].get(str(v['linha']), '—')}`",
                                ", ".join(a for a in v["acoes"] if a), v["num_jobs"], v["duracao_total_s"],
                                ", ".join(v["stages"]) or "—"] for v in script["vinculos_log"]]))
        else:
            md.append(f"ℹ️ {script['vinculos_status']}")

    # ---------------------------------------------------------------- gargalos
    if analise:
        md.append("## Gargalos apontados pela IA")
        if not analise.gargalos:
            md.append("Nenhum gargalo apontado.")
        for g in analise.gargalos:
            md.append(f"### {g.id} · {ICONE_PRIO[g.prioridade]} — {g.titulo}")
            rel = f"  _(achado relacionado: {g.achado_relacionado})_" if g.achado_relacionado else ""
            md.append(f"{g.indicacao}{rel}")
            md.append(_tabela(["Evidência", "Valor citado", "Valor no resumo", "Conferência"],
                              [[f"`{c['caminho']}`", c["citado"], c["real"], SELO[c["status"]]]
                               for c in conferencia["gargalos"][g.id]]))

        md.append("## Recomendações")
        if analise.recomendacoes:
            md.append(_tabela(["#", "Tipo", "Recomendação", "Baseada em"],
                              [[r.id, "🔎 investigar" if r.tipo == "investigar" else "🛠️ otimizar",
                                r.texto, ", ".join(r.sustentada_por)] for r in analise.recomendacoes]))
        else:
            md.append("Nenhuma recomendação.")

    # ------------------------------------------------------------------- score
    if sc.get("descontos") is not None and sc.get("score") is not None:
        md.append("## Como o score foi calculado")
        md.append("_Começa em 100 e desconta por evidência; cada critério tem um teto. "
                  "Não usa a IA: o mesmo log sempre dá o mesmo score._")
        linhas = [[d["criterio"], f"-{d['pontos']:g}" + (" (teto)" if d["limitado_pelo_teto"] else ""),
                   d["calculo"]] for d in sc["descontos"]]
        md.append(_tabela(["Critério", "Desconto", "Cálculo"], linhas or [["nenhum", "0", "—"]]))

    # ------------------------------------------------- achados deterministicos
    md.append("## Achados das regras automáticas")
    md.append("_Detectados por regras fixas, sem IA. Hipóteses são possibilidades, não causas confirmadas._")
    # Usa o relatorio COMPLETO (o resumo pode estar no perfil compacto, sem textos).
    achados = [{"id": f"A{i}", **a} for i, a in enumerate(relatorio.get("oportunidades", []), start=1)] \
        or resumo.get("achados", [])
    if achados:
        md.append(_tabela(["#", "Severidade", "Achado", "O que investigar"],
                          [[a["id"], ICONE_SEV.get(a["severidade"], a["severidade"]), a["titulo"],
                            a.get("investigar")] for a in achados]))
    else:
        md.append("Nenhum achado.")

    # ------------------------------------------------------------------ falhas
    if fa.get("executor_lost_total") or fa.get("stage_failures_total") or fa.get("tasks_falhas_por_motivo"):
        md.append("## Falhas durante a execução")
        if fa.get("executor_lost"):
            md.append("**Executores perdidos**")
            md.append(_tabela(["Horário", "Executor", "Motivo"],
                              [[e["ts"], e["executor"], e["motivo"]] for e in fa["executor_lost"]]))
        if fa.get("stage_failures"):
            md.append("**Stages que falharam**")
            md.append(_tabela(["Horário", "Stage", "Tipo", "Motivo"],
                              [[e["ts"], e["stage_id"], e["tipo_stage"], e["motivo"]] for e in fa["stage_failures"]]))
        if fa.get("tasks_falhas_por_motivo"):
            md.append("**Tasks que falharam, por motivo**")
            md.append(_tabela(["Motivo", "Tasks"], [[k, v] for k, v in fa["tasks_falhas_por_motivo"].items()]))

    # ------------------------------------------------------------------ stages
    md.append("## Stages mais relevantes")
    md.append(_tabela(
        ["Stage", "Tipo", "Duração", "Tasks", "Mediana (ms)", "Máx (ms)", "Skew", "Falhas", "Origem no código"],
        [[s["stage_key"], s.get("tipo_stage"), _duracao(s.get("duration_s")), s.get("num_tasks"),
          s.get("task_ms_mediana"), s.get("task_ms_max"), s.get("skew_ratio"), s.get("num_tasks_falhas"),
          s.get("origem_acao")] for s in resumo.get("stages", [])]))
    om = resumo.get("omitidos", {})
    if om.get("stages"):
        md.append(f"_{om['stages']} stage(s) menores omitidos; veja o JSON completo._")

    # ------------------------------------------------------------- limitacoes
    md.append("## Limitações desta análise")
    indisponiveis = relatorio.get("metricas_nao_disponiveis") or resumo.get("metricas_nao_disponiveis") or {}
    if isinstance(indisponiveis, dict):
        limites = [f"{k}: {v.get('motivo')}" for k, v in indisponiveis.items()]
    else:
        limites = [f"{k}: indisponivel" for k in indisponiveis]
    if analise:
        limites += analise.dados_insuficientes
    limites += [f"[{a['severidade']}] {a['mensagem']}" for a in cons.get("alertas", [])]
    md.append("\n".join(f"- {_c(x)}" for x in limites) or "- Nenhuma.")

    return "\n\n".join(md) + "\n"