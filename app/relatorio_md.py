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


def _abre(titulo: str) -> str:
    return f"<details>\n<summary><b>{titulo}</b></summary>"


FECHA = "</details>"


def _o_que_fazer(analise, resumo: dict) -> list[str]:
    """Tabela principal: o que fazer, onde, quanto custa e como confirmar."""
    cands = {c["id"]: c for c in (resumo.get("plano") or {}).get("candidatas", [])}
    out = []
    if analise and analise.recomendacoes:
        gs = {g.id: g for g in analise.gargalos}
        ordem_g = {g.id: i for i, g in enumerate(analise.gargalos)}
        linhas = []
        for r in sorted(analise.recomendacoes, key=lambda r: min(ordem_g.get(g, 99) for g in r.sustentada_por)):
            g = gs.get(r.sustentada_por[0])
            c = cands.get(g.candidata) if g and g.candidata else None
            custo = f"{c['custo_s']} s" if c else "—"
            linhas.append([ICONE_PRIO.get(g.prioridade, "—") if g else "—",
                           ("🛠️ " if r.tipo == "otimizar" else "🔎 ") + r.texto,
                           ", ".join(map(str, r.linhas_script or [])) or "—", custo,
                           (g.como_confirmar if g else None) or "—"])
        out.append("## O que fazer")
        out.append(_tabela(["Prioridade", "Ação", "Linhas do script", "Custo observado", "Como confirmar"], linhas))
    elif cands:
        out.append("## O que fazer (candidatas, sem IA)")
        out.append("_Montadas por regras determinísticas a partir do log, do plano e do script; ordenadas por custo._")
        out.append(_tabela(["#", "Ação", "Linhas do script", "Custo observado", "Confiança", "Como confirmar"],
                           [[c["id"], c["acao"], ", ".join(str(v) for v in _linhas_onde(c["onde"])) or "—",
                             f"{c['custo_s']} s", c["confianca"], c["como_confirmar"]] for c in cands.values()]))
    return out


def _linhas_onde(onde: dict) -> list[int]:
    linhas = []
    for k, v in onde.items():
        if k == "trecho":
            continue
        for x in (v if isinstance(v, list) else [v]):
            if isinstance(x, int) and x not in linhas:
                linhas.append(x)
    return linhas


def _fases_md(resumo: dict) -> list[str]:
    fases = (resumo.get("plano") or {}).get("fases") or []
    if not fases:
        return []
    linhas = []
    for f in fases:
        trecho = f.get("linhas_script")
        trecho = f"{trecho[0] or 'início'}–{trecho[1] or 'fim'}" if trecho else "—"
        destaque = []
        if f.get("maior_stage") and f["maior_stage"].get("duration_s"):
            m = f["maior_stage"]
            destaque.append(f"stage {m['stage_key']}: {m['num_tasks']} tasks, {m['duration_s']} s")
        if f.get("achados"):
            destaque.append("achados " + ", ".join(f["achados"]))
        ops = [f"{o['metodo']} (l. {o['linha']})" for o in f.get("operacoes", [])
               if o["grupo"] in ("manutencao_delta", "acao_no_driver", "escrita")]
        if ops:
            destaque.append(", ".join(ops))
        ocup = f"{f['ocupacao_pct']} %" if f.get("ocupacao_pct") is not None else "—"
        linhas.append([f["id"], f"{f['de'][:40]} → {f['ate'][:40]}", trecho, f"{f['duracao_s']} s",
                       f["jobs"], ocup, "; ".join(destaque) or "—"])
    return ["## Onde o tempo foi",
            "_Fases delimitadas pelas mensagens do próprio script no stdout._",
            _tabela(["Fase", "Entre as mensagens", "Linhas", "Duração", "Jobs", "Ocupação", "Destaque"], linhas)]


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

    classe = NOME_CLASSE.get(analise.classificacao.value) if analise else "Análise por IA não executada"
    conf_txt = ""
    if analise:
        t_ = conferencia["totais"]
        conf_txt = f" · evidências da IA: {t_['confere']}/{sum(t_.values())} conferem"
    md.append(f"**{classe}** · score {texto_score} · {_duracao(ex.get('duracao_total_s'))} · exit code "
              f"{ex.get('exit_code')} · dados {'OK' if cons.get('consistencia_ok') else 'COM ERROS'}{conf_txt}")
    if analise and not classif["confere"]:
        md.append(f"⚠️ **A classificação da IA diverge do critério do prompt:** esperado "
                  f"`{classif['esperada']}`, a IA respondeu `{classif['obtida']}`.")

    md += _o_que_fazer(analise, resumo)
    md += _fases_md(resumo)

    # ------------------------------------------------------------- visao geral
    md.append(_abre("Visão geral completa"))
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
        t = conferencia["totais"]
        md.append(f"**Conferência das evidências da IA:** {t['confere']} conferem, {t['diverge']} divergem, "
                  f"{t['inexistente']} não existem no resumo ({conferencia['pct_confere']}% conferem).")
    md.append(FECHA)

    # ------------------------------------------------------------------ script
    script = resumo.get("script")
    if script:
        s = script["sanitizacao"]
        md.append(_abre("Script analisado"))
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
        md.append(FECHA)

    # ------------------------------------------------------------------ plano
    plano = resumo.get("plano")
    if plano:
        md.append(_abre("Plano de execução e cruzamentos"))
        f = plano["fonte"]
        coer = f.get("coerencia_com_stderr", {})
        md.append(f"`{f['arquivo']}` · {f['planos_encontrados']} plano(s) físico(s) · coerência de horários "
                  f"com o stderr: **{coer.get('status')}**")
        for a in f.get("avisos", []):
            md.append(f"> ⚠️ {a}")
        for p_ in plano.get("planos", []):
            ops = ", ".join(f"{k} {v}" for k, v in p_.get("operadores", {}).items()
                            if k not in ("Project", "Filter", "Sort", "AdaptiveSparkPlan"))
            md.append(f"**Plano {p_.get('indice')}** · partições de shuffle: "
                      f"{', '.join(map(str, p_.get('particoes_shuffle') or [])) or '—'} · {ops}")
        sinais = {s["id"]: s for s in plano.get("sinais", [])}
        if plano.get("cruzamentos"):
            md.append("**Cruzamentos plano × log × script** _(determinísticos, sem IA)_")
            linhas = []
            for x in plano["cruzamentos"]:
                s = sinais.get(x["sinal"], {})
                log = x.get("log") or {}
                sc = x.get("script") or {}
                custo = (f"{log.get('duracao_stages_s')} s em {len(log.get('stages', []))} stage(s)"
                         if log.get("stages") else "—")
                local = []
                if sc.get("dataframe_reutilizado"):
                    usos = ", ".join(str(u["linha"]) for u in sc.get("usado_por", []))
                    local.append(f"`{sc['dataframe_reutilizado']}` (l. {sc.get('linha_definicao')}) usado nas "
                                 f"linhas {usos}; cache: {'sim' if sc.get('cache_ou_persist') else 'não'}")
                for k, v in sc.items():
                    if k.startswith("linhas_") and v:
                        local.append(f"{k.replace('linhas_', '')}: {', '.join(map(str, v))}")
                    if k == "joins_sort_merge_por_chave":
                        local += [f"join {c}: {', '.join(map(str, ls)) or '—'}" for c, ls in v.items()]
                linhas.append([x["id"], s.get("regra"), x.get("ligacao_plano_log", "—"), custo,
                               x.get("ligacao_plano_script", "—"), "; ".join(local) or sc.get("observacao") or "—"])
            md.append(_tabela(["#", "Sinal do plano", "Ligação c/ log", "Custo no log",
                               "Ligação c/ script", "Local no script"], linhas))
        md.append(FECHA)

    # ---------------------------------------------------------------- gargalos
    if analise:
        md.append(_abre(f"Gargalos da IA e evidências conferidas ({len(analise.gargalos)})"))
        if not analise.gargalos:
            md.append("Nenhum gargalo apontado.")
        for g in analise.gargalos:
            md.append(f"### {g.id} · {ICONE_PRIO[g.prioridade]} — {g.titulo}")
            refs = [x for x in (g.achado_relacionado, g.candidata) if x]
            rel = f"  _(origem: {', '.join(refs)})_" if refs else ""
            md.append(f"{g.indicacao}{rel}")
            if g.como_confirmar:
                md.append(f"**Como confirmar:** {g.como_confirmar}")
            md.append(_tabela(["Evidência", "Valor citado", "Valor no resumo", "Conferência"],
                              [[f"`{c['caminho']}`", c["citado"], c["real"], SELO[c["status"]]]
                               for c in conferencia["gargalos"][g.id]]))

        md.append(FECHA)

    # ------------------------------------------------------------------- score
    if sc.get("descontos") is not None and sc.get("score") is not None:
        md.append(_abre("Como o score foi calculado"))
        md.append("_Começa em 100 e desconta por evidência; cada critério tem um teto. "
                  "Não usa a IA: o mesmo log sempre dá o mesmo score._")
        linhas = [[d["criterio"], f"-{d['pontos']:g}" + (" (teto)" if d["limitado_pelo_teto"] else ""),
                   d["calculo"]] for d in sc["descontos"]]
        md.append(_tabela(["Critério", "Desconto", "Cálculo"], linhas or [["nenhum", "0", "—"]]))
        md.append(FECHA)

    # ------------------------------------------------- achados deterministicos
    md.append(_abre("Achados das regras automáticas"))
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
    md.append(FECHA)

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
    md.append(_abre("Stages mais relevantes"))
    md.append(_tabela(
        ["Stage", "Tipo", "Duração", "Tasks", "Mediana (ms)", "Máx (ms)", "Skew", "Falhas", "Origem no código"],
        [[s["stage_key"], s.get("tipo_stage"), _duracao(s.get("duration_s")), s.get("num_tasks"),
          s.get("task_ms_mediana"), s.get("task_ms_max"), s.get("skew_ratio"), s.get("num_tasks_falhas"),
          s.get("origem_acao")] for s in resumo.get("stages", [])]))
    om = resumo.get("omitidos", {})
    if om.get("stages"):
        md.append(f"_{om['stages']} stage(s) menores omitidos; veja o JSON completo._")
    md.append(FECHA)

    # ------------------------------------------------------------- limitacoes
    md.append(_abre("Limitações desta análise"))
    indisponiveis = relatorio.get("metricas_nao_disponiveis") or resumo.get("metricas_nao_disponiveis") or {}
    if isinstance(indisponiveis, dict):
        limites = [f"{k}: {v.get('motivo')}" for k, v in indisponiveis.items()]
    else:
        limites = [f"{k}: indisponivel" for k in indisponiveis]
    if analise:
        limites += analise.dados_insuficientes
    limites += [f"[{a['severidade']}] {a['mensagem']}" for a in cons.get("alertas", [])]
    md.append("\n".join(f"- {_c(x)}" for x in limites) or "- Nenhuma.")
    md.append(FECHA)

    return "\n\n".join(md) + "\n"