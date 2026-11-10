# Relatório de execução — REF_IDSAC_ASSISTENCIA

_Log `stderr_IDSAC.txt` · gerado em 2026-11-10T15:00:42-03:00 · parser 0.4.0 · prompt analise_v4 (openai:gpt-6-luna)_

**🟡 Degradada** · score **78/100** (🟡 atenção) · 1 min 35 s · exit code 0 · dados OK · evidências da IA: 32/32 conferem

## O que fazer

| Prioridade | Ação | Linhas do script | Custo observado | Como confirmar |
|---|---|---|---|---|
| 🔴 Alta | 🔎 Avaliar tirar vacuum e generate do caminho crítico do job, movendo-os para rotina separada ou menor frequência, no trecho entre as linhas 517 e 522. | 520, 521 | 17.0 s | Rodar sem vacuum (ou com ele em rotina separada) e comparar a duração da fase F6 (17.0 s) e os jobs/stages dela no log. |
| 🟠 Média | 🛠️ Persistir atendimento_genesis ao fim da definição (linha 311) e liberar com unpersist após a última ação que o usa. | 311 | 16.341 s | Na próxima execução o log deve ter 1 scan de 54 arquivos em vez de 3, e o plano deve trocar as leituras por InMemoryTableScan. |
| 🟠 Média | 🔎 Verificar o número de partições do DataFrame criado na linha 186 e, se for alto para poucas linhas, reduzir com coalesce(1) antes do broadcast. | 186 | 6.974 s | Executar df.rdd.getNumPartitions() no DataFrame da linha 186; após o ajuste, os stages de 400 tasks devem cair para 1 task. |
| 🟠 Média | 🔎 Identificar a tabela de origem e contar arquivos e tamanho médio na leitura da linha 214 antes de decidir por compactação. | 214 | — | Identificar a tabela lida no scan e verificar, com DESCRIBE DETAIL, a contagem de arquivos e o tamanho médio. |
| 🟠 Média | 🔎 Comparar as durações dos jobs com o tempo total e examinar os intervalos sem stages, incluindo a fase F1, antes de propor mudança no código. | — | — | Comparar as durações dos jobs em call_sites com a duração total e conferir os executores que realmente tiveram tasks. |

## Onde o tempo foi

_Fases delimitadas pelas mensagens do próprio script no stdout._

| Fase | Entre as mensagens | Linhas | Duração | Jobs | Destaque |
|---|---|---|---|---|---|
| F1 | inicio do log (stderr) → TRU_RECLAMACAO_ATENDIMENTO carregada | início–199 | 30.0 s | 0 | — |
| F2 | TRU_RECLAMACAO_ATENDIMENTO carregada → TRU_DOM_DATA carregada | 199–210 | 6.0 s | 1 | stage 0.0: 7 tasks, 2.564 s |
| F3 | TRU_DOM_DATA carregada → REP_WORKER_ATENDIMENTO_ASSISTENCIA_GENES | 210–220 | 1.0 s | 0 | — |
| F4 | REP_WORKER_ATENDIMENTO_ASSISTENCIA_GENES → INÍCIO ESCRITA DA REFINED NO LAKE | 220–513 | 1.0 s | 0 | — |
| F5 | INÍCIO ESCRITA DA REFINED NO LAKE → ESCRITA DA REFINED NO LAKE EXECUTADA COM | 513–517 | 40.0 s | 36 | stage 35.0: 54 tasks, 6.092 s; save (l. 515) |
| F6 | ESCRITA DA REFINED NO LAKE EXECUTADA COM → Manifesto criado com sucesso! | 517–522 | 17.0 s | 25 | stage 104.0: 10000 tasks, 8.543 s; achados A1; vacuum (l. 520), generate (l. 521) |

<details>
<summary><b>Visão geral completa</b></summary>

| Indicador | Valor |
|---|---|
| Score de qualidade (determinístico) | **78/100** (🟡 atenção) |
| Classificação (IA) | 🟡 Degradada |
| Duração total | 1 min 35 s |
| Código de saída | 0 |
| Spark | 3.5.6-amzn-1 |
| Executores com tasks / máximo | 3 / 3 |
| Ocupação estimada do cluster | 39.9 % |
| Executores perdidos | 0 |
| Falhas de stage | 0 |
| Retentativas de task | 0 |
| Consistência dos dados | OK |

**Por que essa classificação:** A execução terminou com exit_code 0, mas há achado ALTA (A1) e achados MEDIA (A2 e A3).

**Conferência das evidências da IA:** 32 conferem, 0 divergem, 0 não existem no resumo (100.0% conferem).

</details>

<details>
<summary><b>Script analisado</b></summary>

`IDSAC.py` · 523 linhas no arquivo · enviado em modo **completo** (462 linhas) · sha256 `368e20e0576b…`

_Antes do envio: 18 comentário(s) removido(s), 0 e-mail(s) e 0 credencial(is) mascarados; paths e hosts com os mesmos pseudônimos do log._

ℹ️ Nenhum call site do log aponta para este arquivo (a DataFrame API do PySpark costuma registrar NativeMethodAccessorImpl.java:0). A ligacao stage -> linha do script NAO pode ser afirmada a partir do log.

</details>

<details>
<summary><b>Plano de execução e cruzamentos</b></summary>

`stdout_IDSAC.txt` · 1 plano(s) físico(s) · coerência de horários com o stderr: **ok**

> ⚠️ Nomes de tabela truncados no plano ('...'): aumente spark.sql.maxMetadataStringLength no job para o nome sair completo.

> ⚠️ Plano impresso ANTES da execucao (isFinalPlan=false): o AQE pode ter trocado estrategias de join e coalescido particoes durante a execucao.

**Plano 1** · partições de shuffle: 400 · Exchange 13, HashAggregate 10, SortAggregate 10, FileScan 5, BroadcastHashJoin 4, BroadcastExchange 4, ScanExistingRDD 4, SortMergeJoin 2, Expand 2, WindowGroupLimit 2, Union 1, Window 1

**Cruzamentos plano × log × script** _(determinísticos, sem IA)_

| # | Sinal do plano | Ligação c/ log | Custo no log | Ligação c/ script | Local no script |
|---|---|---|---|---|---|
| X1 | tabela_lida_varias_vezes | multiplicidade | 16.341 s em 3 stage(s) | nome | `atendimento_genesis` (l. 271) usado nas linhas 321, 340, 412; cache: não |
| X2 | broadcast_de_dataframe_em_memoria | tipo | 7.442 s em 5 stage(s) | nome | create_dataframe: 186 |
| X3 | shuffles | valor | 3.274 s em 6 stage(s) | sem_correspondencia | spark.sql.shuffle.partitions nao e definido neste script (vem da sessao/cluster ou de modulo externo) |
| X4 | expand_count_distinct | — | — | nome | count_distinct: 342, 417, 420 |
| X5 | sort_aggregate | — | — | nome | drop_duplicates: 308; first: 342 |
| X6 | joins | — | — | nome | join COD_PROTOCOLO: 350; join DAT_REFERENCIA: 449; join: 248, 287, 350, 449; broadcast_explicito: 249, 288 |
| X7 | janelas | — | — | nome | window: 313 |

</details>

<details>
<summary><b>Gargalos da IA e evidências conferidas (5)</b></summary>

### G1 · 🔴 Alta — Manutenção Delta no trecho após a escrita

A fase F6, entre a conclusão da escrita e a criação do manifesto, durou 17.0 s e inclui vacuum e generate; o stage 104.0 teve 10000 tasks e durou 8.543 s. Isso sugere que a manutenção Delta contribui para o tempo do trecho, mas não atribui o stage a uma operação específica.  _(origem: A1, C1)_

**Como confirmar:** Rodar sem vacuum (ou com ele em rotina separada) e comparar a duração da fase F6 (17.0 s) e os jobs/stages dela no log.

| Evidência | Valor citado | Valor no resumo | Conferência |
|---|---|---|---|
| `plano.candidatas[C1].custo_s` | 17.0 | 17.0 | ✅ confere |
| `plano.fases[F6].duracao_s` | 17.0 | 17.0 | ✅ confere |
| `achados[A1].evidencia.num_tasks` | 10000 | 10000 | ✅ confere |
| `achados[A1].evidencia.duration_s` | 8.543 | 8.543 | ✅ confere |
| `script.linhas.520` | delta_table.vacuum(72) | delta_table.vacuum(72) | ✅ confere |
| `script.linhas.521` | delta_table.generate("symlink_format_manifest") | delta_table.generate("symlink_format_manifest") | ✅ confere |

### G2 · 🟠 Média — Releituras de atendimento_genesis

O plano lê REP_WORKER_A... 3 vezes, correspondendo a 3 scans de 54 arquivos no log e 16.341 s de stages. O cruzamento X1 liga essas leituras à definição de atendimento_genesis na linha 271 e aos usos nas linhas 321, 340 e 412.  _(origem: C2)_

**Como confirmar:** Na próxima execução o log deve ter 1 scan de 54 arquivos em vez de 3, e o plano deve trocar as leituras por InMemoryTableScan.

| Evidência | Valor citado | Valor no resumo | Conferência |
|---|---|---|---|
| `plano.candidatas[C2].custo_s` | 16.341 | 16.341 | ✅ confere |
| `plano.cruzamentos[X1].log.scans` | 3 | 3 | ✅ confere |
| `plano.cruzamentos[X1].log.arquivos_por_scan` | 54 | 54 | ✅ confere |
| `plano.cruzamentos[X1].log.duracao_stages_s` | 16.341 | 16.341 | ✅ confere |
| `plano.sinais[S1.1].leituras` | 3 | 3 | ✅ confere |
| `script.linhas.271` | atendimento_genesis = ( | atendimento_genesis = ( | ✅ confere |
| `script.linhas.311` | ) | ) | ✅ confere |

### G3 · 🟠 Média — Broadcast de DataFrame criado em memória

Dois stages com 400 tasks são compatíveis com a materialização de broadcasts; a candidata associa a eles 6.974 s. Como a ligação do log ao plano é do tipo, isso sugere conferir as partições do DataFrame criado na linha 186.  _(origem: C3)_

**Como confirmar:** Executar df.rdd.getNumPartitions() no DataFrame da linha 186; após o ajuste, os stages de 400 tasks devem cair para 1 task.

| Evidência | Valor citado | Valor no resumo | Conferência |
|---|---|---|---|
| `plano.candidatas[C3].custo_s` | 6.974 | 6.974 | ✅ confere |
| `plano.candidatas[C3].onde.create_dataframe[0]` | 186 | 186 | ✅ confere |
| `plano.cruzamentos[X2].log.tasks_por_stage[0]` | 400 | 400 | ✅ confere |
| `plano.cruzamentos[X2].log.tasks_por_stage[1]` | 400 | 400 | ✅ confere |
| `plano.sinais[S1.4].ocorrencias` | 4 | 4 | ✅ confere |
| `script.linhas.186` | spark.createDataFrame( |     spark.createDataFrame( | ✅ confere |

### G4 · 🟠 Média — Scans com muitos arquivos por leitura

O log registra 3 scans, com até 54 arquivos em um scan, e o cruzamento X1 relaciona esses scans a 16.341 s de stages. Isso sugere investigar arquivos pequenos na origem, embora o custo de abertura não esteja separado do custo das releituras de G2.  _(origem: A2)_

**Como confirmar:** Identificar a tabela lida no scan e verificar, com DESCRIBE DETAIL, a contagem de arquivos e o tamanho médio.

| Evidência | Valor citado | Valor no resumo | Conferência |
|---|---|---|---|
| `achados[A2].evidencia.ocorrencias` | 3 | 3 | ✅ confere |
| `achados[A2].evidencia.max_arquivos_em_um_scan` | 54 | 54 | ✅ confere |
| `plano.cruzamentos[X1].log.duracao_stages_s` | 16.341 | 16.341 | ✅ confere |
| `script.linhas.214` | .load(rep_wrk_att_path) |     .load(rep_wrk_att_path) | ✅ confere |

### G5 · 🟠 Média — Baixa ocupação estimada durante a execução

As tasks ocuparam 39.9% da capacidade estimada; a fase F1 durou 30.0 s sem stages. Isso pode indicar tempo fora da execução de tasks, mas não identifica por si só a causa.  _(origem: A3)_

**Como confirmar:** Comparar as durações dos jobs em call_sites com a duração total e conferir os executores que realmente tiveram tasks.

| Evidência | Valor citado | Valor no resumo | Conferência |
|---|---|---|---|
| `achados[A3].evidencia.ocupacao_pct` | 39.9 | 39.9 | ✅ confere |
| `execucao.duracao_total_s` | 95.0 | 95.0 | ✅ confere |
| `plano.fases[F1].duracao_s` | 30.0 | 30.0 | ✅ confere |
| `plano.fases[F1].stages` | 0 | 0 | ✅ confere |
| `plano.fases[F1].tempo_em_stages_s` | 0 | 0 | ✅ confere |

</details>

<details>
<summary><b>Achados das regras automáticas</b></summary>

_Detectados por regras fixas, sem IA. Hipóteses são possibilidades, não causas confirmadas._

| # | Severidade | Achado | O que investigar |
|---|---|---|---|
| A1 | 🔴 ALTA | Stage 104.0 com 10000 tasks de mediana 9.0 ms | Numero de particoes do DataFrame antes da acao em origem.acao; spark.sql.shuffle.partitions e repartition/coalesce explicitos no script. |
| A2 | 🟠 MÉDIA | Scan com muitos arquivos e particao maxima menor que o custo de abertura | Identificar a tabela lida nesse scan (o log nao traz o caminho) e contar arquivos/tamanho medio com DESCRIBE DETAIL. |
| A3 | 🟠 MÉDIA | Tasks ocuparam 39.9% da capacidade estimada | Comparar a soma das duracoes dos jobs (call_sites) com a duracao total e conferir quantos executores realmente subiram (executores_com_tasks). |
| A4 | 🟡 BAIXA | 23 jobs disparados por $anonfun$recordDeltaOperationInternal$1 at DatabricksLogging.scala:128 | Quantas leituras/escritas Delta o script faz na mesma tabela e se DeltaTable.history() ou forName() sao chamados repetidamente. |
| A5 | 🟡 BAIXA | 5 jobs disparados por $anonfun$withThreadLocalCaptured$1 at FutureTask.java:264 | Quantas leituras/escritas Delta o script faz na mesma tabela e se DeltaTable.history() ou forName() sao chamados repetidamente. |
| A6 | ⚪ INFO | 37 shuffle(s) pequenos para o paralelismo: alvo do AQE abaixo do advisory | So vale aprofundar se houver tambem stage_explodido ou muitas tasks curtas logo apos esses shuffles. |

</details>

<details>
<summary><b>Stages mais relevantes</b></summary>

| Stage | Tipo | Duração | Tasks | Mediana (ms) | Máx (ms) | Skew | Falhas | Origem no código |
|---|---|---|---|---|---|---|---|---|
| 104.0 | ShuffleMapStage | 9 s | 10000 | 9.0 | 156 | 17.33 | 0 | $anonfun$recordDeltaOperationInternal$1 at DatabricksLogging.scala:128 |
| 35.0 | ShuffleMapStage | 6 s | 54 | 48.0 | 3473 | 72.35 | 0 | save at NativeMethodAccessorImpl.java:0 |
| 32.0 | ShuffleMapStage | 6 s | 1 | 1269 | 1269 | 1.0 | 0 | save at NativeMethodAccessorImpl.java:0 |
| 34.0 | ShuffleMapStage | 5 s | 54 | 63.0 | 2845 | 45.16 | 0 | save at NativeMethodAccessorImpl.java:0 |
| 33.0 | ShuffleMapStage | 5 s | 54 | 76.0 | 3202 | 42.13 | 0 | save at NativeMethodAccessorImpl.java:0 |
| 31.0 | ResultStage | 4 s | 400 | 54.0 | 85 | 1.57 | 0 | $anonfun$withThreadLocalCaptured$1 at FutureTask.java:264 |
| 36.0 | ShuffleMapStage | 4 s | 47 | 50 | 2518 | 50.36 | 0 | save at NativeMethodAccessorImpl.java:0 |
| 45.0 | ShuffleMapStage | 4 s | 200 | 109.0 | 389 | 3.57 | 0 | save at NativeMethodAccessorImpl.java:0 |
| 48.0 | ShuffleMapStage | 3 s | 100 | 72.0 | 291 | 4.04 | 0 | save at NativeMethodAccessorImpl.java:0 |
| 51.0 | ShuffleMapStage | 3 s | 60 | 115.0 | 311 | 2.7 | 0 | save at NativeMethodAccessorImpl.java:0 |

_53 stage(s) menores omitidos; veja o JSON completo._

</details>

<details>
<summary><b>Limitações desta análise</b></summary>

- gc_time_ratio: Log em nivel INFO padrao, sem eventos de GC verboso (flag -verbose:gc nao habilitada).
- shuffle_read_write_bytes: Log expoe apenas sinal indireto de shuffle (advisory/actual target size), nao bytes lidos/escritos reais.
- spill_memoria_disco: Nao ha eventos de spill no log neste nivel de verbosidade.
- result_size_driver: Result Size por task so existe no event log (SparkListenerTaskEnd). Sem ele, o risco de collect e estimado por numero de particoes, nao por bytes.
- O stage 104.0 é atribuído pelo log a código interno do engine; a fase F6 o localiza no trecho de manutenção, mas não permite atribuí-lo a vacuum ou generate individualmente.
- O log não separa o custo de abertura dos arquivos do custo das releituras de G2.
- A ligação entre os stages de broadcast e o log é do tipo; a origem exata dos jobs não está confirmada.
- [info] 1 job(s) terminaram sem a linha '..., took X s'; a duracao deles fica fora de call_sites.

</details>
