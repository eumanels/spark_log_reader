from __future__ import annotations
from .padroes import LIMIARES
from .parser import LogEvent

ORDEM_SEVERIDADE = {"ALTA": 0, "MEDIA": 1, "BAIXA": 2, "INFO": 3}


def _achado(regra: str, severidade: str, titulo: str, *, evidencia: dict, limiar_usado: dict,
            hipoteses: list[str], investigar: str, opcoes_se_confirmado: list[str] | None = None) -> dict:
    assert severidade in ORDEM_SEVERIDADE, severidade
    return {
        "regra": regra,
        "severidade": severidade,
        "titulo": titulo,
        "evidencia": evidencia,
        "limiar_usado": limiar_usado,
        "hipoteses": hipoteses,
        "investigar": investigar,
        "opcoes_se_confirmado": opcoes_se_confirmado or [],
    }


def _origem_resumida(s: dict) -> dict | None:
    o = s.get("origem")
    if not o:
        return None
    return {
        "job_id": o.get("job_id"),
        "tipo_job": o.get("tipo_job"),
        "acao": o.get("acao"),
        "codigo_interno_do_engine": bool((o.get("call_site") or {}).get("interno")),
    }


def detectar_oportunidades(eventos: list[LogEvent], stage_summary: list[dict],
                           call_sites: list[dict], capacidade: dict) -> list[dict]:
    achados = []

    # 1. Window sem partitionBy --------------------------------------------------------
    # A propria mensagem do Spark afirma o fato ("Moving all data to a single partition").
    janelas = [e for e in eventos if e.event_type == "window_sem_particao"]
    if janelas:
        achados.append(_achado(
            "window_sem_particao", "ALTA",
            "Window function sem partitionBy (o Spark avisou que moveu tudo para uma particao)",
            evidencia={"ocorrencias": len(janelas), "primeiro_ts": janelas[0].ts,
                       "ultimo_ts": janelas[-1].ts},
            limiar_usado={"ocorrencias_minimas": 1},
            hipoteses=[
                "Pode haver Window.orderBy(...) sem partitionBy(...) no codigo, ex.: row_number() "
                "para numerar o DataFrame inteiro.",
                "Pode ser uma janela global intencional; o impacto depende do volume, que este "
                "log nao informa.",
            ],
            investigar="Procurar Window.orderBy sem partitionBy no script e cruzar o horario "
                       "primeiro_ts/ultimo_ts com os jobs em call_sites.",
            opcoes_se_confirmado=[
                "Adicionar partitionBy na chave de negocio, se a numeracao nao precisa ser global.",
                "Se precisa ser global: avaliar monotonically_increasing_id ou zipWithIndex.",
            ],
        ))

    # 2. Stage com muitas tasks triviais --------------------------------------------------
    for s in stage_summary:
        n = s["num_tasks_submetidas"] or s["num_tasks"]
        mediana = s["task_duration_ms"]["median"]
        if mediana is None:
            continue  # tentativa sem nenhuma task concluida
        if n >= LIMIARES["stage_explodido_tasks"] and mediana < LIMIARES["stage_explodido_mediana_ms"]:
            origem = _origem_resumida(s)
            interno = bool(origem and origem["codigo_interno_do_engine"])
            hipoteses = [
                "Pode haver particoes demais para o volume de dados do stage: o custo vira "
                "agendamento de task, nao processamento.",
                "Pode ser um stage sem shuffle (leitura/escrita), onde o AQE nao reduz o numero "
                "de particoes.",
            ]
            if interno:
                hipoteses.append(
                    "Pode ser uma operacao de manutencao do Delta (ex.: checkpoint, estatisticas) "
                    "e nao uma transformacao do script: a origem e codigo interno do engine.")
            achados.append(_achado(
                "stage_explodido", "ALTA",
                f"Stage {s['stage_key']} com {n} tasks de mediana {mediana} ms",
                evidencia={
                    "stage_key": s["stage_key"],
                    "tipo_stage": s.get("tipo_stage"),
                    "num_tasks": n,
                    "mediana_ms": mediana,
                    "tempo_total_task_s": round(s["task_duration_ms"]["total"] / 1000, 1),
                    "duration_s": s.get("duration_s"),
                    "rdd_desc": s.get("rdd_desc"),
                    "origem": origem,
                },
                limiar_usado={"stage_explodido_tasks": LIMIARES["stage_explodido_tasks"],
                              "stage_explodido_mediana_ms": LIMIARES["stage_explodido_mediana_ms"]},
                hipoteses=hipoteses,
                investigar="Numero de particoes do DataFrame antes da acao em origem.acao; "
                           "spark.sql.shuffle.partitions e repartition/coalesce explicitos no script.",
                opcoes_se_confirmado=[
                    "coalesce/repartition antes da escrita.",
                    "Ajustar spark.sql.shuffle.partitions ou deixar o AQE coalescer (stage com shuffle).",
                ],
            ))

    # 3. Alvo do AQE abaixo do advisory ---------------------------------------------------
    # Pelo codigo do Spark (ShufflePartitionsUtil.coalescePartitions):
    #   alvo = max(min(ceil(total_shuffle / min_particoes), advisory), minimo)
    # Logo, alvo < advisory significa que total_shuffle / min_particoes < advisory: o
    # shuffle e pequeno em relacao ao paralelismo minimo. Isso e informacao, nao defeito.
    alvos = [e for e in eventos if e.event_type == "aqe_coalesce_target"]
    abaixo = [
        e for e in alvos
        if int(e.data["actual_bytes"]) > 0
        and int(e.data["advisory_bytes"]) / int(e.data["actual_bytes"]) >= LIMIARES["aqe_razao_alvo"]
    ]
    if abaixo:
        no_minimo = sum(1 for e in abaixo if e.data["actual_bytes"] == e.data["min_bytes"])
        razoes = sorted(int(e.data["advisory_bytes"]) / int(e.data["actual_bytes"]) for e in abaixo)
        ex = abaixo[0].data
        achados.append(_achado(
            "aqe_alvo_reduzido", "INFO",
            f"{len(abaixo)} shuffle(s) pequenos para o paralelismo: alvo do AQE abaixo do advisory",
            evidencia={
                "ocorrencias": len(abaixo),
                "total_de_shuffles_com_coalesce": len(alvos),
                "ocorrencias_com_alvo_no_minimo": no_minimo,
                "advisory_bytes": int(ex["advisory_bytes"]),
                "min_partition_bytes": int(ex["min_bytes"]),
                "razao_advisory_sobre_alvo_min": round(razoes[0], 1),
                "razao_advisory_sobre_alvo_max": round(razoes[-1], 1),
            },
            limiar_usado={"aqe_razao_alvo": LIMIARES["aqe_razao_alvo"]},
            hipoteses=[
                "Pode ser o comportamento padrao do AQE (coalescePartitions.parallelismFirst=true): "
                "ele prefere manter paralelismo a atingir o tamanho advisory.",
                "Pode indicar shuffles de volume pequeno; quando o alvo fica no minimo, o volume "
                "por particao minima ja e menor que minPartitionSize.",
            ],
            investigar="So vale aprofundar se houver tambem stage_explodido ou muitas tasks curtas "
                       "logo apos esses shuffles.",
            opcoes_se_confirmado=[
                "Avaliar spark.sql.adaptive.coalescePartitions.parallelismFirst=false para respeitar o advisory.",
            ],
        ))

    # 4. Mesma acao disparando muitos jobs ------------------------------------------------
    for cs in call_sites:
        if cs["num_jobs"] >= LIMIARES["acao_repetida_min_jobs"]:
            interno = bool(cs.get("call_site") and cs["call_site"].get("interno"))
            if interno:
                hipoteses = [
                    "Pode ser consulta de metadado do Delta (history, snapshot, log) repetida a "
                    "cada operacao do script.",
                ]
                investigar = ("Quantas leituras/escritas Delta o script faz na mesma tabela e se "
                              "DeltaTable.history() ou forName() sao chamados repetidamente.")
                opcoes = ["Ler o metadado uma vez e reaproveitar o resultado no driver."]
            else:
                hipoteses = [
                    "Pode haver uma acao (count, collect, save...) dentro de um laco no driver.",
                    "Pode ser o mesmo DataFrame sendo recalculado em cada acao, sem cache.",
                ]
                investigar = (f"Linha {cs['call_site']['linha'] if cs.get('call_site') else '?'} "
                              f"de {cs['call_site']['arquivo'] if cs.get('call_site') else '?'}: "
                              "verificar se esta dentro de laco e se o DataFrame base e reutilizado.")
                opcoes = ["Juntar as iteracoes numa unica transformacao (join/agregacao).",
                          "Cachear o DataFrame base se ele for reutilizado."]
            achados.append(_achado(
                "acao_repetida", "BAIXA" if interno else "MEDIA",
                f"{cs['num_jobs']} jobs disparados por {cs['acao'][:70]}",
                evidencia={
                    "acao": cs["acao"],
                    "num_jobs": cs["num_jobs"],
                    "duracao_total_s": cs["duracao_total_s"],
                    "codigo_interno_do_engine": interno,
                },
                limiar_usado={"acao_repetida_min_jobs": LIMIARES["acao_repetida_min_jobs"]},
                hipoteses=hipoteses,
                investigar=investigar,
                opcoes_se_confirmado=opcoes,
            ))

    # 5. Skew de task ---------------------------------------------------------------------
    for s in stage_summary:
        # O piso absoluto e essencial: sem ele, um stage de 9 tasks com mediana de 100 ms
        # e um outlier de 3 s vira "skew 29x" - razao enorme, impacto nenhum.
        d = s["task_duration_ms"]
        if s["skew_ratio"] and s["skew_ratio"] >= LIMIARES["skew_ratio"] \
           and s["num_tasks"] >= LIMIARES["skew_min_tasks"] \
           and d["max"] >= LIMIARES["skew_min_max_ms"]:
            achados.append(_achado(
                "skew_task", "MEDIA",
                f"Stage {s['stage_key']}: task mais lenta {s['skew_ratio']}x a mediana",
                evidencia={
                    "stage_key": s["stage_key"],
                    "tipo_stage": s.get("tipo_stage"),
                    "skew_ratio": s["skew_ratio"],
                    "max_ms": d["max"],
                    "p95_ms": d["p95"],
                    "median_ms": d["median"],
                    "num_tasks": s["num_tasks"],
                    "duration_s": s.get("duration_s"),
                    "origem": _origem_resumida(s),
                },
                limiar_usado={"skew_ratio": LIMIARES["skew_ratio"],
                              "skew_min_tasks": LIMIARES["skew_min_tasks"],
                              "skew_min_max_ms": LIMIARES["skew_min_max_ms"]},
                hipoteses=[
                    "Pode haver uma chave concentrada em join/groupBy (se o stage vem de shuffle).",
                    "Pode haver um arquivo de entrada maior que os demais (se o stage e de leitura).",
                    "Pode ser um executor mais lento; comparar executor_duration_ms do stage.",
                ],
                investigar="Ver tipo_stage e origem.acao; contar registros por chave no DataFrame "
                           "do join/groupBy ou o tamanho dos arquivos lidos.",
                opcoes_se_confirmado=[
                    "Chave concentrada: salting ou spark.sql.adaptive.skewJoin.enabled (joins).",
                    "Arquivo grande: compactar/reparticionar a origem.",
                ],
            ))

    # 6. Maioria dos stages com 1 task ----------------------------------------------------
    if stage_summary:
        um_task = [s for s in stage_summary if s["num_tasks"] == 1]
        pct = 100 * len(um_task) / len(stage_summary)
        if pct >= LIMIARES["stage_1_task_pct"]:
            achados.append(_achado(
                "fragmentacao_stages", "BAIXA",
                f"{round(pct)}% dos stages rodaram com uma unica task",
                evidencia={"stages_com_1_task": len(um_task), "total_stages": len(stage_summary),
                           "pct": round(pct, 1)},
                limiar_usado={"stage_1_task_pct": LIMIARES["stage_1_task_pct"]},
                hipoteses=[
                    "Pode haver muitas acoes pequenas no script, cada uma virando um job.",
                    "Pode ser trabalho de metadado do Delta (log de transacoes), que costuma rodar em 1 task.",
                ],
                investigar="Ver em call_sites quais acoes geram mais jobs e se sao do script ou internas.",
                opcoes_se_confirmado=["Consolidar transformacoes antes de disparar acoes."],
            ))

    # 7. Scan com muitos arquivos pequenos --------------------------------------------------
    scans = [e for e in eventos if e.event_type == "scan_bin_packing"]
    small = [
        e for e in scans
        if int(e.data["num_files"]) >= LIMIARES["small_files_min"]
        and int(e.data["max_split_bytes"]) <= int(e.data["open_cost_bytes"])
    ]
    if small:
        achados.append(_achado(
            "small_files", "MEDIA",
            "Scan com muitos arquivos e particao maxima menor que o custo de abertura",
            evidencia={"ocorrencias": len(small),
                       "max_arquivos_em_um_scan": max(int(e.data["num_files"]) for e in small),
                       "max_split_bytes": int(small[0].data["max_split_bytes"]),
                       "open_cost_bytes": int(small[0].data["open_cost_bytes"])},
            limiar_usado={"small_files_min": LIMIARES["small_files_min"]},
            hipoteses=[
                "Pode haver uma tabela de origem com muitos arquivos pequenos (sem compactacao).",
            ],
            investigar="Identificar a tabela lida nesse scan (o log nao traz o caminho) e contar "
                       "arquivos/tamanho medio com DESCRIBE DETAIL.",
            opcoes_se_confirmado=["OPTIMIZE / compactacao na tabela de origem."],
        ))

    # 8. Ocupacao baixa do cluster --------------------------------------------------------
    if capacidade.get("ocupacao_pct") is not None and \
       capacidade["ocupacao_pct"] < LIMIARES["ocupacao_baixa_pct"]:
        achados.append(_achado(
            "ocupacao_baixa", "MEDIA",
            f"Tasks ocuparam {capacidade['ocupacao_pct']}% da capacidade estimada",
            evidencia={k: capacidade[k] for k in
                       ("slots", "tempo_total_task_s", "capacidade_total_s", "ocupacao_pct")},
            limiar_usado={"ocupacao_baixa_pct": LIMIARES["ocupacao_baixa_pct"]},
            hipoteses=[
                "Pode haver tempo de driver entre jobs (planejamento, metadado Delta, commit).",
                "Pode haver mais executores que o necessario para o volume.",
                "Pode ser que a ocupacao real seja maior: a capacidade usa o TETO de executores "
                "x duracao, e com alocacao dinamica nem todos sobem ao mesmo tempo.",
            ],
            investigar="Comparar a soma das duracoes dos jobs (call_sites) com a duracao total e "
                       "conferir quantos executores realmente subiram (executores_com_tasks).",
            opcoes_se_confirmado=["Reduzir acoes/consultas de metadado no driver.",
                                  "Rever spark.dynamicAllocation.maxExecutors."],
        ))

    return sorted(achados, key=lambda a: ORDEM_SEVERIDADE[a["severidade"]])