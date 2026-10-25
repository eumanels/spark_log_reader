# Desafio - Análise de Logs Spark EMR com IA Generativa

> **Primeiro commit:** 27/07/2026  
> Início do repositório do desafio.

---

# Escopo

## Objetivo

Desenvolver uma solução que, a partir do log **stderr** de uma execução **Spark no EMR** (plano lógico, execução por executor, uso de memória, RDDs, estágios e tasks), produza um relatório técnico estruturado contendo:

- Classificação da execução;
- Métricas de desempenho;
- Gargalos identificados;
- Recomendações de otimização;
- Score de qualidade.

A solução deve utilizar um assistente de **IA Generativa** apenas como camada de raciocínio sobre dados previamente estruturados, **nunca sobre o log bruto**.

---

# Contexto

O `stderr` é o log gerado pelo próprio engine Spark e contém informações como:

- Plano lógico;
- Execução por executor;
- Uso de memória;
- RDDs;
- Ciclo de vida de estágios (stages) e tasks;
- Exceptions;
- Status final da aplicação.

Esse log concentra praticamente todos os sinais de performance da aplicação, mas também apresenta desafios importantes:

1. Pode atingir grandes volumes em execuções longas;
2. Contém eventos multilinha (stack traces e mensagens quebradas);
3. Quando agregado pelo YARN, pode reunir logs de múltiplos executores/containers em um único arquivo.

> **Importante:** o candidato deve documentar explicitamente as premissas assumidas sobre a versão do Spark/EMR e o nível de verbosidade do log utilizado.

---

# Objetivo Técnico

A partir do `stderr`, utilizando investigação técnica e IA Generativa, a solução deve ser capaz de:

- Extrair e estruturar eventos e métricas relevantes;
- Segmentar corretamente por executor/container, quando necessário;
- Calcular métricas derivadas de performance;
- Utilizar um assistente de IA para interpretar o resumo estruturado e gerar:
  - classificação;
  - gargalos;
  - recomendações;
- Calcular um score de qualidade de forma determinística;
- Validar a própria análise antes de considerá-la concluída;
- Tratar dados potencialmente sensíveis antes de qualquer envio ao assistente.

---

# Escopo do Desafio

## Etapa 1 — Entendimento do Problema

- Mapear a estrutura do `stderr`;
- Documentar as premissas assumidas:
  - versão do Spark/EMR;
  - nível de verbosidade;
  - log single-container ou agregado;
- Identificar os principais eventos:
  - início/fim de estágio;
  - duração das tasks;
  - GC;
  - shuffle;
  - spill;
  - exceptions;
  - status final.

---

## Etapa 2 — Parsing e Estruturação

Implementar:

- Extração estruturada dos eventos (regex, parser dedicado ou biblioteca especializada);
- Tratamento de eventos multilinha (stack traces e mensagens quebradas);
- Segmentação por executor/container quando o log estiver agregado;
- Mascaramento/redação de dados sensíveis antes do envio ao assistente;
- Arquitetura escalável, sem assumir que todo o arquivo cabe em memória (streaming/chunking quando apropriado).

---

## Etapa 3 — Agregação e Feature Engineering

Implementar, no mínimo, as seguintes métricas:

- Duração total da execução;
- Duração por estágio;
- GC Time Ratio;
- Shuffle Read;
- Shuffle Write;
- Spill em memória;
- Spill em disco;
- Skew (razão entre a task mais lenta e a mediana do estágio);
- Executor Lost;
- Task Retries;
- Stage Failures.

---

## Etapa 4 — Uso da IA Generativa

O assistente deve receber **apenas o resumo estruturado e mascarado**, nunca o log bruto.

A IA deverá gerar:

- Classificação da execução;
- Gargalos priorizados com evidências;
- Recomendações específicas de otimização.

Além disso, o candidato deve documentar pelo menos **um caso** em que:

- discordou da IA;
- corrigiu uma sugestão;
- descartou uma recomendação.

Isso servirá como evidência de senso crítico.

---

## Etapa 5 — Scoring

Implementar um **Score de Qualidade (0–100)** utilizando uma metodologia:

- determinística;
- documentada;
- reproduzível.

---

## Etapa 6 — Validação da Solução

Realizar validações por meio de:

- Comparação entre a leitura manual dos três logs de referência e a saída do pipeline;
- Relato das divergências encontradas;
- Medição da precisão e cobertura da extração automática frente a uma amostra anotada manualmente;
- Demonstração da solução utilizando um log inédito (holdout).

---

## Etapa 7 — Entregáveis

O projeto deverá conter:

### Código

- Repositório Git organizado em módulos.

### README.md

Deve conter:

- Como executar o projeto;
- Premissas de versão e verbosidade;
- Decisões de arquitetura;
- Justificativa das métricas utilizadas;
- Metodologia do Score de Qualidade;
- Resultado da validação;
- Casos em que o assistente foi corrigido ou descartado.

### Relatórios

Gerar um relatório para:

- Os 3 logs de referência;
- O log de validação (holdout).

---

# Como executar

```bash
pip install -r requirements.txt

# Menu interativo (rode da raiz do repositorio): lista os logs de logs/,
# pergunta o modo de IA e qual script de scripts/ enviar junto
python menu.py

# Linha de comando
python main.py --log logs/<log>.txt --ia manual            # gera o prompt para colar no chat
python main.py --log logs/<log>.txt --script scripts/<script>.py --ia anthropic
python main.py --log logs/<log>.txt --script scripts/<script>.py --ia groq
```

## Analise com o script executado (`--script`)

O log continua **nunca** indo bruto para a IA. Com `--script`, o codigo `.py` que rodou no
EMR entra no `*_resumo_ia.json`, na chave `script`, e a IA passa a usar o prompt
`prompts/analise_v2.MD` (o `analise_v1` continua intacto e e usado sem script).

Antes do envio o script passa por (`app/script_fonte.py`):

1. **remocao de comentarios** via `tokenize` (preserva `#` dentro de strings e a numeracao das linhas);
2. **o mesmo `Sanitizador` do log**: paths S3, ARNs, IPs e hosts EC2 viram `path_N`/`host_N`
   com os mesmos pseudonimos do resumo;
3. **mascaras de codigo**: e-mails e literais atribuidos a `password`, `secret`, `token`,
   `api_key`, `access_key`... e chaves AWS (`AKIA...`).

**Ligacao log -> codigo (`script.vinculos_log`)**: quando o call site de um job/stage aponta
para o arquivo do script (ex.: `count at etl.py:12`), a linha fica ligada aos jobs, a duracao
e aos stages. Na DataFrame API o PySpark costuma registrar `NativeMethodAccessorImpl.java:0`;
nesse caso nao ha ligacao e o prompt proibe a IA de afirma-la.

**Modos**: `completo` (padrao ate 600 linhas) ou `trechos` (linhas citadas pelo log +-3 e
linhas com operacoes Spark +-1). No limite de tokens o pipeline reduz primeiro o resumo e
depois o script, nesta ordem: padrao/completo -> compacto/completo -> padrao/trechos ->
compacto/trechos -> minimo/trechos.

**Evidencia no codigo**: a IA cita linhas como `script.linhas.<numero>` com o texto exato;
a conferencia do relatorio marca a linha como "confere" ou "diverge", como qualquer metrica.
Todo gargalo ainda precisa de pelo menos uma evidencia vinda das metricas.

**Limitacoes**: nomes de buckets montados por concatenacao (`"s3://" + bucket`), nomes de
pessoas fora de comentarios e dados de negocio em strings nao sao mascarados; revise o
script antes de usar um provedor externo.
