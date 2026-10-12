from __future__ import annotations

# Mensagens que tem padrao proprio em padroes.py. Se aparecerem em
# raw_warn_error, a regex correspondente nao casou com o formato real do log.
PREFIXOS_COM_PADRAO = {
    "Lost executor ": "executor_lost",
    "Lost task ": "task_failed",
}

FRACAO_MINIMA_CABECALHO = 0.5   # abaixo disso o log nao parece log4j em texto
TOLERANCIA_DURACAO_S = 2.0      # timestamps do log tem precisao de 1 s


def _alerta(codigo: str, severidade: str, mensagem: str, esperado=None, obtido=None) -> dict:
    a = {"codigo": codigo, "severidade": severidade, "mensagem": mensagem}
    if esperado is not None or obtido is not None:
        a["esperado"] = esperado
        a["obtido"] = obtido
    return a


def validar_relatorio(r: dict) -> dict:
    c = r.get("eventos_esparsos", {})
    n = lambda chave: c.get(chave, 0)  # noqa: E731
    stages = r.get("stages", [])
    alertas: list[dict] = []
    checagens = 0

    # ---------------------------------------------------------------- leitura
    checagens += 1
    leitura = r.get("leitura_do_log") or {}
    total = leitura.get("linhas_total")
    if total == 0:
        alertas.append(_alerta("LOG_VAZIO", "erro", "O arquivo de log nao tem linhas."))
    elif total:
        fracao = leitura.get("linhas_com_cabecalho", 0) / total
        if fracao < FRACAO_MINIMA_CABECALHO:
            alertas.append(_alerta(
                "FORMATO_INESPERADO", "erro",
                f"So {fracao:.0%} das linhas tem cabecalho log4j. O log pode estar em outro "
                "formato (ex.: logging estruturado em JSON) e quase nada foi extraido.",
                esperado=f">= {FRACAO_MINIMA_CABECALHO:.0%}", obtido=f"{fracao:.0%}"))

    checagens += 1
    if (r.get("duracao_total_execucao") or {}).get("duration_s") is None:
        alertas.append(_alerta("SEM_DURACAO_TOTAL", "erro",
                               "Nenhum timestamp pode ser interpretado; duracao total indisponivel."))

    # ----------------------------------------------------------- status final
    checagens += 1
    if not r.get("status_final"):
        alertas.append(_alerta(
            "SEM_STATUS_FINAL", "aviso",
            "Nao ha linha 'SparkContext is stopping with exitCode'. A execucao pode ter "
            "sido interrompida ou o log foi truncado."))

    # -------------------------------------------------------------------- jobs
    checagens += 1
    sem_fim = n("job_start") - n("job_concluido") - n("job_failed")
    if sem_fim > 0:
        alertas.append(_alerta(
            "JOBS_SEM_FIM", "aviso", f"{sem_fim} job(s) iniciados sem registro de fim.",
            esperado=n("job_start"), obtido=n("job_concluido") + n("job_failed")))
    elif sem_fim < 0:
        alertas.append(_alerta(
            "JOBS_FIM_SEM_INICIO", "erro", "Ha mais jobs encerrados do que iniciados.",
            esperado=n("job_start"), obtido=n("job_concluido") + n("job_failed")))

    checagens += 1
    sem_duracao = n("job_concluido") - n("job_finish") - n("job_failed")
    if sem_duracao > 0:
        alertas.append(_alerta(
            "JOBS_SEM_DURACAO", "info",
            f"{sem_duracao} job(s) terminaram sem a linha '..., took X s'; a duracao deles "
            "fica fora de call_sites."))

    # ------------------------------------------------------------------ stages
    checagens += 1
    encerrados = n("stage_finish") + n("stage_failed")
    if n("stage_submit") > encerrados:
        alertas.append(_alerta(
            "STAGES_SEM_FIM", "aviso",
            f"{n('stage_submit') - encerrados} stage(s) submetidos sem 'finished' nem 'failed'.",
            esperado=n("stage_submit"), obtido=encerrados))
    elif n("stage_submit") < encerrados:
        alertas.append(_alerta(
            "STAGES_FIM_SEM_INICIO", "erro", "Ha mais stages encerrados do que submetidos.",
            esperado=n("stage_submit"), obtido=encerrados))

    checagens += 1
    if n("stage_tasks_submitted") != n("stage_submit"):
        alertas.append(_alerta(
            "SUBMISSOES_NAO_RECONHECIDAS", "erro",
            "Linhas 'Submitting N missing tasks' e 'Submitting <Stage>' nao fecham; "
            "alguma mensagem (provavelmente multilinha) nao casou com a regex.",
            esperado=n("stage_submit"), obtido=n("stage_tasks_submitted")))

    checagens += 1
    falhas_relatorio = (r.get("stage_failures") or {}).get("total", 0)
    falhas_stages = sum(1 for s in stages if s.get("status_tentativa") == "falhou")
    if falhas_relatorio != falhas_stages:
        alertas.append(_alerta(
            "FALHAS_DE_STAGE_DIVERGENTES", "erro",
            "stage_failures.total difere do numero de tentativas com status 'falhou'.",
            esperado=falhas_relatorio, obtido=falhas_stages))

    checagens += 1
    total_s = (r.get("duracao_total_execucao") or {}).get("duration_s")
    if total_s is not None:
        longos = [s["stage_key"] for s in stages
                  if s.get("duration_s") is not None
                  and s["duration_s"] > total_s + TOLERANCIA_DURACAO_S]
        if longos:
            alertas.append(_alerta(
                "STAGE_MAIS_LONGO_QUE_APLICACAO", "erro",
                f"Stage(s) com duracao maior que a aplicacao inteira: {', '.join(longos[:10])}."))

    checagens += 1
    sem_origem = [s["stage_key"] for s in stages if not s.get("origem")]
    if sem_origem:
        alertas.append(_alerta(
            "STAGES_SEM_ORIGEM", "aviso",
            f"{len(sem_origem)} stage(s) sem job de origem ({', '.join(sem_origem[:10])}); "
            "a ligacao stage -> codigo do usuario fica incompleta."))

    # ------------------------------------------------------------------- tasks
    checagens += 1
    terminadas = n("task_finish") + n("task_failed")
    if n("task_start") > terminadas:
        alertas.append(_alerta(
            "TASKS_SEM_FIM", "aviso",
            f"{n('task_start') - terminadas} task(s) iniciadas sem 'Finished' nem 'Lost'.",
            esperado=n("task_start"), obtido=terminadas))
    elif n("task_start") < terminadas:
        alertas.append(_alerta(
            "TASKS_FIM_SEM_INICIO", "erro", "Ha mais tasks encerradas do que iniciadas.",
            esperado=n("task_start"), obtido=terminadas))

    checagens += 1
    soma_concluidas = sum(s.get("num_tasks", 0) for s in stages)
    if soma_concluidas != n("task_finish"):
        alertas.append(_alerta(
            "TASKS_FORA_DOS_STAGES", "erro",
            "A soma de num_tasks dos stages nao bate com as linhas 'Finished task'.",
            esperado=n("task_finish"), obtido=soma_concluidas))

    checagens += 1
    soma_falhas = sum(s.get("num_tasks_falhas", 0) for s in stages)
    if soma_falhas != n("task_failed"):
        alertas.append(_alerta(
            "FALHAS_DE_TASK_FORA_DOS_STAGES", "erro",
            "A soma de num_tasks_falhas dos stages nao bate com as linhas 'Lost task'.",
            esperado=n("task_failed"), obtido=soma_falhas))

    # ------------------------------------------- eventos conhecidos nao lidos
    checagens += 1
    nao_lidos: dict[str, int] = {}
    for e in r.get("raw_warn_error", []):
        msg = (e.get("data") or {}).get("msg", "")
        for prefixo, tipo in PREFIXOS_COM_PADRAO.items():
            if msg.startswith(prefixo):
                nao_lidos[tipo] = nao_lidos.get(tipo, 0) + 1
    for tipo, qtd in nao_lidos.items():
        alertas.append(_alerta(
            "EVENTO_CONHECIDO_NAO_RECONHECIDO", "erro",
            f"{qtd} linha(s) de '{tipo}' ficaram em raw_warn_error: a regex '{tipo}' "
            "nao casou com o formato real do log.",
            esperado=0, obtido=qtd))

    ordem = {"erro": 0, "aviso": 1, "info": 2}
    alertas.sort(key=lambda a: ordem[a["severidade"]])
    contagem = {sev: sum(1 for a in alertas if a["severidade"] == sev) for sev in ordem}
    return {
        "ok": contagem["erro"] == 0,
        "checagens_executadas": checagens,
        "erros": contagem["erro"],
        "avisos": contagem["aviso"],
        "infos": contagem["info"],
        "alertas": alertas,
    }