# Relatório de execução — REF_ACIONAMENTO_TRACKING

_Log `log_REF_TRACKING_sucesso.txt` · gerado em 2026-10-12T12:27:01-03:00 · parser 0.4.0 · prompt analise_v1 (groq:qwen/qwen3.8-27b)_

## Visão geral

| Indicador | Valor |
|---|---|
| Score de qualidade (determinístico) | **83/100** (🟢 boa) |
| Classificação (IA) | 🟡 Degradada |
| Duração total | 5 min 25 s |
| Código de saída | 0 |
| Spark | 3.5.6-amzn-1 |
| Executores com tasks / máximo | 3 / 3 |
| Ocupação estimada do cluster | 57.8 % |
| Executores perdidos | 0 |
| Falhas de stage | 0 |
| Retentativas de task | 0 |
| Consistência dos dados | OK |

**Por que essa classificação:** A execucao terminou com exit_code 0, mas ha um achado de severidade ALTA (A1) e um achado de severidade MEDIA (A2).

**Conferência das evidências da IA:** 8 conferem, 0 divergem, 0 não existem no resumo (100.0% conferem).

## Gargalos apontados pela IA

### G1 · 🔴 Alta — Stage 92.0 com 10000 tasks de mediana 8.0 ms

Pode indicar particoes excessivas para o volume de dados processado no stage, gerando overhead de scheduling. O skew_ratio elevado (39.12) sugere distribuicao desigual de trabalho, embora a mediana seja baixa. Sugere investigar a origem do particionamento (acao interna do engine Delta).  _(achado relacionado: A1)_

| Evidência | Valor citado | Valor no resumo | Conferência |
|---|---|---|---|
| `achados[A1].evidencia.num_tasks` | 10000 | 10000 | ✅ confere |
| `achados[A1].evidencia.mediana_ms` | 8.0 | 8.0 | ✅ confere |
| `stages[92.0].skew_ratio` | 39.12 | 39.12 | ✅ confere |

### G2 · 🟠 Média — Ocupacao baixa de 57.8% da capacidade estimada

Sugere que os executores ficaram ociosos por parte do tempo, possivelmente devido a gargalos de I/O, dependencias entre stages ou particoes insuficientes para saturar os slots. Ha indicio de subutilizacao da infraestrutura.  _(achado relacionado: A2)_

| Evidência | Valor citado | Valor no resumo | Conferência |
|---|---|---|---|
| `achados[A2].evidencia.ocupacao_pct` | 57.8 | 57.8 | ✅ confere |
| `execucao.ocupacao_pct` | 57.8 | 57.8 | ✅ confere |

## Recomendações

| # | Tipo | Recomendação | Baseada em |
|---|---|---|---|
| R1 | 🔎 investigar | Investigar a origem do particionamento do stage 92.0 (acao interna do engine Delta) para entender por que 10000 tasks foram geradas com mediana de 8ms, avaliando se coalesce ou re-particionamento e viavel. | G1 |
| R2 | 🔎 investigar | Investigar as causas da baixa ocupacao (57.8%), verificando se ha gargalos de I/O, dependencias sequenciais entre stages ou se o numero de particoes e insuficiente para aproveitar os 3 slots disponiveis. | G2 |

## Como o score foi calculado

_Começa em 100 e desconta por evidência; cada critério tem um teto. Não usa a IA: o mesmo log sempre dá o mesmo score._

| Critério | Desconto | Cálculo |
|---|---|---|
| achados | -17 | acao_repetida=2 + ocupacao_baixa=5 + stage_explodido=10 |

## Achados das regras automáticas

_Detectados por regras fixas, sem IA. Hipóteses são possibilidades, não causas confirmadas._

| # | Severidade | Achado | O que investigar |
|---|---|---|---|
| A1 | 🔴 ALTA | Stage 92.0 com 10000 tasks de mediana 8.0 ms | Numero de particoes do DataFrame antes da acao em origem.acao; spark.sql.shuffle.partitions e repartition/coalesce explicitos no script. |
| A2 | 🟠 MÉDIA | Tasks ocuparam 57.8% da capacidade estimada | Comparar a soma das duracoes dos jobs (call_sites) com a duracao total e conferir quantos executores realmente subiram (executores_com_tasks). |
| A3 | 🟡 BAIXA | 8 jobs disparados por getHistory at DeltaTableOperations.scala:57 | Quantas leituras/escritas Delta o script faz na mesma tabela e se DeltaTable.history() ou forName() sao chamados repetidamente. |
| A4 | 🟡 BAIXA | 22 jobs disparados por $anonfun$recordDeltaOperationInternal$1 at DatabricksLogging.scala:128 | Quantas leituras/escritas Delta o script faz na mesma tabela e se DeltaTable.history() ou forName() sao chamados repetidamente. |
| A5 | 🟡 BAIXA | 8 jobs disparados por $anonfun$withThreadLocalCaptured$1 at FutureTask.java:264 | Quantas leituras/escritas Delta o script faz na mesma tabela e se DeltaTable.history() ou forName() sao chamados repetidamente. |
| A6 | ⚪ INFO | 19 shuffle(s) pequenos para o paralelismo: alvo do AQE abaixo do advisory | So vale aprofundar se houver tambem stage_explodido ou muitas tasks curtas logo apos esses shuffles. |

## Stages mais relevantes

| Stage | Tipo | Duração | Tasks | Mediana (ms) | Máx (ms) | Skew | Falhas | Origem no código |
|---|---|---|---|---|---|---|---|---|
| 51.0 | ShuffleMapStage | 41 s | 8 | 12639.0 | 17850 | 1.41 | 0 | save at NativeMethodAccessorImpl.java:0 |
| 92.0 | ShuffleMapStage | 41 s | 10000 | 8.0 | 313 | 39.12 | 0 | $anonfun$recordDeltaOperationInternal$1 at DatabricksLogging.scala:128 |
| 53.0 | ShuffleMapStage | 33 s | 7 | 133 | 251 | 1.89 | 0 | save at NativeMethodAccessorImpl.java:0 |
| 43.0 | ShuffleMapStage | 23 s | 5 | 11078 | 13683 | 1.24 | 0 | save at NativeMethodAccessorImpl.java:0 |
| 47.0 | ShuffleMapStage | 20 s | 5 | 2499 | 3331 | 1.33 | 0 | save at NativeMethodAccessorImpl.java:0 |

_74 stage(s) menores omitidos; veja o JSON completo._

## Limitações desta análise

- gc_time_ratio: Log em nivel INFO padrao, sem eventos de GC verboso (flag -verbose:gc nao habilitada).
- shuffle_read_write_bytes: Log expoe apenas sinal indireto de shuffle (advisory/actual target size), nao bytes lidos/escritos reais.
- spill_memoria_disco: Nao ha eventos de spill no log neste nivel de verbosidade.
- result_size_driver: Result Size por task so existe no event log (SparkListenerTaskEnd). Sem ele, o risco de collect e estimado por numero de particoes, nao por bytes.
- GC Time Ratio indisponivel no log de driver em nivel INFO.
- Bytes de shuffle (leitura/escrita) indisponiveis, impedindo analise de volume de dados transferidos.
- Spill de memoria/disco indisponivel, impedindo analise de pressao de memoria.
- [info] 1 job(s) terminaram sem a linha '..., took X s'; a duracao deles fica fora de call_sites.
