# Relatório de execução — REP_APOLICE_ITEM_MAXPAR_SANDBOX

_Log `LOG_REP_APOLICE9.txt` · gerado em 2026-10-25T17:46:02-03:00 · parser 0.4.0 · prompt analise_v2 (groq:openai/gpt-oss-120b)_

## Visão geral

| Indicador | Valor |
|---|---|
| Score de qualidade (determinístico) | **70/100** (🟡 atenção) |
| Classificação (IA) | 🟡 Degradada |
| Duração total | 8 min 18 s |
| Código de saída | 0 |
| Spark | 3.5.6-amzn-1 |
| Executores com tasks / máximo | 10 / 10 |
| Ocupação estimada do cluster | 75.2 % |
| Executores perdidos | 0 |
| Falhas de stage | 0 |
| Retentativas de task | 0 |
| Consistência dos dados | OK |

**Por que essa classificação:** A execucao terminou com exit_code 0, mas ha achados de severidade ALTA (A1, A2).

**Conferência das evidências da IA:** 19 conferem, 0 divergem, 0 não existem no resumo (100.0% conferem).

## Script analisado

`REP_APOLICE_ITEM_MAXPAR_SANDBOX.py` · 498 linhas no arquivo · enviado em modo **trechos** (62 linhas) · sha256 `45b7e7fd1ccc…`

_Antes do envio: 32 comentário(s) removido(s), 0 e-mail(s) e 0 credencial(is) mascarados; paths e hosts com os mesmos pseudônimos do log._

ℹ️ Nenhum call site do log aponta para este arquivo (a DataFrame API do PySpark costuma registrar NativeMethodAccessorImpl.java:0). A ligacao stage -> linha do script NAO pode ser afirmada a partir do log.

## Gargalos apontados pela IA

### G1 · 🔴 Alta — Stage 255.0 com explosao de tasks

Pode indicar um stage com numero excessivo de tasks, possivelmente causado por um shuffle sem particionamento adequado. Sugere investigar a origem da operacao de shuffle associada ao stage 255.0.  _(achado relacionado: A2)_

| Evidência | Valor citado | Valor no resumo | Conferência |
|---|---|---|---|
| `achados[A2].evidencia.stage_key` | 255.0 | 255.0 | ✅ confere |
| `achados[A2].evidencia.num_tasks` | 10000 | 10000 | ✅ confere |
| `achados[A2].evidencia.mediana_ms` | 31.0 | 31.0 | ✅ confere |
| `achados[A2].evidencia.duration_s` | 8.043 | 8.043 | ✅ confere |

### G2 · 🔴 Alta — Uso de window sem particionamento

Pode indicar que operacoes de window estao sendo executadas sem especificar uma coluna de particionamento adequada, levando a alto custo. Sugere investigar o uso de Window.partitionBy nas linhas 356-358.  _(achado relacionado: A1)_

| Evidência | Valor citado | Valor no resumo | Conferência |
|---|---|---|---|
| `achados[A1].evidencia.ocorrencias` | 80 | 80 | ✅ confere |
| `script.linhas.356` | janela_combustivel = Window.partitionBy( | janela_combustivel = Window.partitionBy( | ✅ confere |
| `script.linhas.357` | "COD_TABELA_FIPE", "DSC_CODIGO", "NUM_ANO", "DAT_REFERENCIA" |     "COD_TABELA_FIPE", "DSC_CODIGO", "NUM_ANO", "DAT_REFERENCIA" | ✅ confere |
| `script.linhas.358` | ).orderBy( | ).orderBy( | ✅ confere |

### G3 · 🟠 Média — Skew de tasks no stage 224.0

Ha indicio de desequilibrio forte entre tasks, possivelmente causado por dados altamente desbalanceados ou falta de repartition. Sugere investigar a origem da operacao de escrita associada ao stage 224.0.  _(achado relacionado: A8)_

| Evidência | Valor citado | Valor no resumo | Conferência |
|---|---|---|---|
| `achados[A8].evidencia.stage_key` | 224.0 | 224.0 | ✅ confere |
| `achados[A8].evidencia.skew_ratio` | 66.29 | 66.29 | ✅ confere |
| `achados[A8].evidencia.max_ms` | 6960 | 6960 | ✅ confere |
| `achados[A8].evidencia.num_tasks` | 121 | 121 | ✅ confere |

### G4 · 🟠 Média — Skew de tasks no stage 242.0

Skew moderado que pode impactar a duracao do stage. Sugere investigar se a distribuicao dos dados pode ser melhor balanceada ou se um rebalanceamento explicito e necessario.  _(achado relacionado: A9)_

| Evidência | Valor citado | Valor no resumo | Conferência |
|---|---|---|---|
| `achados[A9].evidencia.stage_key` | 242.0 | 242.0 | ✅ confere |
| `achados[A9].evidencia.skew_ratio` | 3.37 | 3.37 | ✅ confere |
| `achados[A9].evidencia.max_ms` | 37499 | 37499 | ✅ confere |
| `achados[A9].evidencia.num_tasks` | 160 | 160 | ✅ confere |

## Recomendações

| # | Tipo | Recomendação | Baseada em |
|---|---|---|---|
| R1 | 🔎 investigar | Analisar a origem do shuffle que gerou o stage 255.0 e considerar adicionar particionamento ou reduzir o numero de tasks. | G1 |
| R2 | 🔎 investigar | Rever o uso de Window.partitionBy nas linhas 356-358 para garantir que a coluna de particionamento seja adequada ao volume de dados. | G2 |
| R3 | 🔎 investigar | Examinar a distribuicao dos dados que alimentam os stages 224.0 e 242.0 e aplicar rebalanceamento ou salting se necessario. | G3, G4 |

## Como o score foi calculado

_Começa em 100 e desconta por evidência; cada critério tem um teto. Não usa a IA: o mesmo log sempre dá o mesmo score._

| Critério | Desconto | Cálculo |
|---|---|---|
| achados | -30 (teto) | acao_repetida=5 + fragmentacao_stages=2 + skew_task=5 + stage_explodido=10 + window_sem_particao=10 |

## Achados das regras automáticas

_Detectados por regras fixas, sem IA. Hipóteses são possibilidades, não causas confirmadas._

| # | Severidade | Achado | O que investigar |
|---|---|---|---|
| A1 | 🔴 ALTA | Window function sem partitionBy (o Spark avisou que moveu tudo para uma particao) | Procurar Window.orderBy sem partitionBy no script e cruzar o horario primeiro_ts/ultimo_ts com os jobs em call_sites. |
| A2 | 🔴 ALTA | Stage 255.0 com 10000 tasks de mediana 31.0 ms | Numero de particoes do DataFrame antes da acao em origem.acao; spark.sql.shuffle.partitions e repartition/coalesce explicitos no script. |
| A3 | 🟠 MÉDIA | 11 jobs disparados por collect at utils_maxpar.zip/utils_maxpar/classe_decisor_incidentes.py: | Linha 298 de utils_maxpar.zip/utils_maxpar/classe_decisor_incidentes.py: verificar se esta dentro de laco e se o DataFrame base e reutilizado. |
| A4 | 🟠 MÉDIA | 8 jobs disparados por showString at NativeMethodAccessorImpl.java:0 | Linha 0 de NativeMethodAccessorImpl.java: verificar se esta dentro de laco e se o DataFrame base e reutilizado. |
| A5 | 🟠 MÉDIA | 8 jobs disparados por first at utils_maxpar.zip/utils_maxpar/classe_decisor_incidentes.py:33 | Linha 331 de utils_maxpar.zip/utils_maxpar/classe_decisor_incidentes.py: verificar se esta dentro de laco e se o DataFrame base e reutilizado. |
| A6 | 🟠 MÉDIA | 5 jobs disparados por collect at utils_maxpar.zip/utils_maxpar/classe_decisor_incidentes.py: | Linha 304 de utils_maxpar.zip/utils_maxpar/classe_decisor_incidentes.py: verificar se esta dentro de laco e se o DataFrame base e reutilizado. |
| A7 | 🟠 MÉDIA | 5 jobs disparados por collect at utils_maxpar.zip/utils_maxpar/classe_decisor_incidentes.py: | Linha 305 de utils_maxpar.zip/utils_maxpar/classe_decisor_incidentes.py: verificar se esta dentro de laco e se o DataFrame base e reutilizado. |
| A8 | 🟠 MÉDIA | Stage 224.0: task mais lenta 66.29x a mediana | Ver tipo_stage e origem.acao; contar registros por chave no DataFrame do join/groupBy ou o tamanho dos arquivos lidos. |
| A9 | 🟠 MÉDIA | Stage 242.0: task mais lenta 3.37x a mediana | Ver tipo_stage e origem.acao; contar registros por chave no DataFrame do join/groupBy ou o tamanho dos arquivos lidos. |
| A10 | 🟡 BAIXA | 7 jobs disparados por $anonfun$withThreadLocalCaptured$1 at FutureTask.java:264 | Quantas leituras/escritas Delta o script faz na mesma tabela e se DeltaTable.history() ou forName() sao chamados repetidamente. |
| A11 | 🟡 BAIXA | 21 jobs disparados por getHistory at DeltaTableOperations.scala:57 | Quantas leituras/escritas Delta o script faz na mesma tabela e se DeltaTable.history() ou forName() sao chamados repetidamente. |
| A12 | 🟡 BAIXA | 16 jobs disparados por $anonfun$recordDeltaOperationInternal$1 at DatabricksLogging.scala:128 | Quantas leituras/escritas Delta o script faz na mesma tabela e se DeltaTable.history() ou forName() sao chamados repetidamente. |
| A13 | 🟡 BAIXA | 53% dos stages rodaram com uma unica task | Ver em call_sites quais acoes geram mais jobs e se sao do script ou internas. |
| A14 | ⚪ INFO | 77 shuffle(s) pequenos para o paralelismo: alvo do AQE abaixo do advisory | So vale aprofundar se houver tambem stage_explodido ou muitas tasks curtas logo apos esses shuffles. |

## Stages mais relevantes

| Stage | Tipo | Duração | Tasks | Mediana (ms) | Máx (ms) | Skew | Falhas | Origem no código |
|---|---|---|---|---|---|---|---|---|

_151 stage(s) menores omitidos; veja o JSON completo._

## Limitações desta análise

- gc_time_ratio: Log em nivel INFO padrao, sem eventos de GC verboso (flag -verbose:gc nao habilitada).
- shuffle_read_write_bytes: Log expoe apenas sinal indireto de shuffle (advisory/actual target size), nao bytes lidos/escritos reais.
- spill_memoria_disco: Nao ha eventos de spill no log neste nivel de verbosidade.
- result_size_driver: Result Size por task so existe no event log (SparkListenerTaskEnd). Sem ele, o risco de collect e estimado por numero de particoes, nao por bytes.
- gc_time_ratio indisponivel
- result_size_driver indisponivel
- shuffle_read_write_bytes indisponivel
- spill_memoria_disco indisponivel
- [info] 1 job(s) terminaram sem a linha '..., took X s'; a duracao deles fica fora de call_sites.
