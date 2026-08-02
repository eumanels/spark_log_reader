# Etapa 1 — Entendimento do Problema

## 1. Estrutura geral da linha do stderr

Todo evento log segue o padrão:

```
DD/MM/AA HH:MM:SS NÍVEL Componente: Mensagem
```

Exemplo real:
```
26/07/26 08:32:09 INFO TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0) ([host]:port, executor 1, partition 0, PROCESS_LOCAL, 9771 bytes)
```

Linhas que não seguem esse padrão (sem o cabeçalho `DD/MM/AA HH:MM:SS NÍVEL Componente:`) são sua grande maioria stack traces (tratadas como continuação da linha estruturada anterior)

## 2. Premissas assumidas

| Item | Valor identificado | Evidência |
|---|---|---|
| Versão do Spark | 3.5.6-amzn-1 | Linha `SparkContext: Running Spark version 3.5.6-amzn-1` |
| Nível de verbosidade | INFO padrão, sem flags de debug adicionais | Falta de eventos de GC detalhado, bytes de shuffle e spill |
| Tipo de log | **Log do driver, não agregado por container YARN** | vários executores aparecem no mesmo arquivo, idenficados campo `executor_id` em cada linha de task, não há segmentação por arquivo/container. 

## 3. Eventos identificados

### Confirmados com evidência

| Evento | Como identificar | Observação |
|---|---|---|
| Início de job | `SparkContext: Starting job: ...` / `DAGScheduler: Got job N (...) with X output partitions` | |
| Início de estágio | `DAGScheduler: Submitting <Tipo>Stage N (...), which has no missing parents` | |
| **Fim de estágio (marcador confiável)** | `TaskSchedulerImpl: Removed TaskSet N.M, whose tasks have all completed` | 
| **Duração de estágio (precisa)** | `DAGScheduler: <Tipo>Stage N (...) finished in D s` | O Spark loga a duração diretamente, com precisão de milissegundo | 
| **Duração de job (precisa)** | `DAGScheduler: Job N finished: <desc>, took D s` | Precisão de microssegundo (6 casas decimais). |
| Início de task | `TaskSetManager: Starting task X.Y in stage S.A (TID T) ([host]:port, executor E, partition P, LOCALITY, B bytes)` | |
| **Duração de task** | `TaskSetManager: Finished task X.Y in stage S.A (TID T) in D ms on ...` | Duração em milissegundos disponível diretamente na mensagem. |
| **Status final da aplicação** | `SparkContext: SparkContext is stopping with exitCode N.` | `exitCode 0` = sucesso. |
| Pressão de recursos / backlog | `ExecutorAllocationManager: Requesting N new executors because tasks are backlogged` | Proxy útil de degradação quando não há GC/spill disponíveis. |
| Registro de executor | `ExecutorMonitor: New executor N has registered (new total is T)` | |
| Uso de memória (broadcast) | `MemoryStore: Block broadcast_N stored as values/bytes in memory (estimated size ..., free ...)` | |
| Sinal indireto de shuffle | `MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle N` / `ShufflePartitionsUtil: For shuffle(N), advisory target size: X` | **Não é bytes lidos/escritos**, é apenas indicação de que houve shuffle e o tamanho-alvo de coalescing. |

## 4. Dicionário de Métricas:

### Skew Ratio

skew_ratio ≈ 1.0 = todas as tasks do estágio demoram parecido. Não há desbalanceamento.
skew_ratio alto (bem acima de 1) = existe pelo menos uma task muito mais lenta que as outras, "segurando" o estágio inteiro. Como o Spark só avança pro próximo estágio quando todas as tasks terminam, uma task 10x mais lenta que a mediana faz o estágio inteiro demorar 10x mais do que precisaria.
