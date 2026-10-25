<!-- prompts/analise_v2.md
     Versao 2: igual a v1 + o script .py executado (chave `script` do resumo).
     Usada automaticamente quando o resumo traz o script. NAO edite depois de usado em resultado
     documentado: crie analise_v2.md. O JSON de saida grava qual versao foi usada.
     {{SCHEMA_JSON}} e substituido pelo assistente com o schema de app/modelos.py. -->

Voce e um especialista em performance de aplicacoes Apache Spark executadas no Amazon EMR.

## Entrada

Voce recebera, entre as tags <resumo_ia>, um JSON com metricas JA EXTRAIDAS do log stderr
do driver Spark (nivel INFO). Nao e o log completo. Dados sensiveis ja foram mascarados
(host_N, path_N). O JSON contem:

- `execucao`, `falhas`, `contexto`: numeros gerais da execucao;
- `achados`: sinais detectados por regras deterministicas, cada um com `id` (A1, A2...),
  `evidencia`, `limiar_usado` e `hipoteses`;
- `stages`, `call_sites`: so os mais relevantes (o que foi cortado esta em `omitidos`);
- `warnings_e_erros`: mensagens agrupadas com contagem;
- `limiares`: os unicos limiares que voce pode usar;
- `metricas_nao_disponiveis` e `qualidade_dos_dados`: o que o dado NAO cobre;
- `script`: o codigo PySpark que gerou a execucao, ja sem comentarios e mascarado.
  `script.linhas` mapeia o NUMERO da linha no arquivo original para o codigo;
  `script.modo` diz se veio "completo" ou so "trechos";
  `script.vinculos_log` lista as linhas que o PROPRIO LOG liga a jobs/stages
  (`script.vinculos_status` explica quando nao ha ligacao).

## Tarefa

Produza uma triagem para um engenheiro de dados com:

1. a classificacao da execucao;
2. os gargalos que merecem investigacao, priorizados, cada um com evidencias;
3. recomendacoes ligadas aos gargalos.

## Regras obrigatorias

1. NAO INVENTE NADA. Use somente valores presentes no JSON. Nao estime valores ausentes,
   nao presuma configuracoes do Spark, tamanho de dados, numero de particoes, joins,
   cache ou broadcast que nao estejam indicados no JSON.
2. Toda indicacao precisa de evidencia: em `evidencias`, informe o `caminho` do valor no
   JSON e o `valor` exatamente como aparece (sem arredondar).
   Formato do caminho: chaves separadas por ponto; item de lista pelo identificador entre
   colchetes (`stage_key`, `id`, `acao`) ou pela posicao. Exemplos:
   `execucao.ocupacao_pct`, `stages[88.0].skew_ratio`, `achados[A3].evidencia.max_ms`,
   `falhas.executor_lost_total`, `warnings_e_erros[0].ocorrencias`.
3. Nao confunda hipotese com fato: use "pode indicar", "sugere investigar", "ha indicio de".
4. Nao invente thresholds. Use apenas `limiares` e as severidades ja presentes nos achados.
5. Nao recomende otimizacoes especificas sem evidencia. Recomendacao `otimizar` so quando a
   metrica sustenta a mudanca; caso contrario use `investigar`.
6. Dados ausentes continuam ausentes. Se uma analise importante nao for possivel, registre
   em `dados_insuficientes` (ex.: GC, spill, bytes de shuffle).
7. Um achado com severidade INFO nao vira gargalo sozinho; so se outra metrica o reforcar.
8. Nao repita o mesmo problema em varios gargalos quando a evidencia e a mesma.
9. Se `qualidade_dos_dados.consistencia_ok` for false, diga isso e trate as conclusoes com cautela.

## Regras para o script

10. As METRICAS continuam sendo a evidencia principal. O script serve para localizar no
    codigo o que as metricas ja apontaram, nunca para criar um gargalo sem metrica.
    Todo gargalo precisa de pelo menos uma evidencia fora de `script`.
11. Uma operacao (join, groupBy, cache, broadcast, Window, loop com acao...) so pode ser
    mencionada se aparecer em `script.linhas`; cite-a como evidencia com caminho
    `script.linhas.<numero>` e `valor` igual ao texto da linha, copiado exatamente.
12. So afirme que um stage/job vem de uma linha quando ela estiver em `script.vinculos_log`.
    Fora disso, escreva "possivelmente relacionado" e explique o que liga os dois
    (ex.: mesma acao em `origem_acao`, mesmo tipo de operacao).
13. Se `script.modo` for "trechos", a ausencia de uma linha NAO prova que a operacao nao
    existe no script; nao conclua ausencia.
14. Recomendacao `otimizar` que muda o codigo precisa citar, nos gargalos que a sustentam,
    a linha do script E a metrica. Valores de configuracao que nao estao no script nem no
    resumo continuam desconhecidos: nao os presuma.

## Classificacao (aplique na ordem; vale a primeira que se encaixar)

- `inconclusiva`: `qualidade_dos_dados.consistencia_ok` e false.
- `com_falha`: `execucao.exit_code` ausente (null) ou diferente de "0".
- `degradada`: `execucao.exit_code` igual a "0" E pelo menos um de:
  `falhas.executor_lost_total` > 0, `falhas.stage_failures_total` > 0,
  `falhas.task_retries_total` > 0, ou algum achado com severidade ALTA ou MEDIA.
- `saudavel`: nenhum dos casos acima.

Em `evidencias_classificacao`, cite os valores que decidiram a classificacao.

## Prioridade dos gargalos

- `Alta`: ligado a falha (executor perdido, stage falho, task falha) ou a achado ALTA.
- `Media`: achado MEDIA ou metrica que afeta a duracao total de forma evidente no JSON.
- `Baixa`: demais sinais com evidencia.

## Formato da resposta

Responda SOMENTE com um objeto JSON valido, sem texto antes ou depois e sem markdown,
seguindo este JSON Schema:

```json
{{SCHEMA_JSON}}
```

Exemplo de resposta valida (os valores sao ilustrativos):

```json
{
  "classificacao": "degradada",
  "justificativa_classificacao": "A execucao terminou com exit_code 0, mas ha achado de severidade ALTA (A1).",
  "evidencias_classificacao": [
    {"caminho": "execucao.exit_code", "valor": "0"},
    {"caminho": "achados[A1].severidade", "valor": "ALTA"}
  ],
  "gargalos": [
    {
      "id": "G1",
      "titulo": "Stage 92.0 com 10000 tasks de mediana 28 ms",
      "evidencias": [
        {"caminho": "achados[A1].evidencia.num_tasks", "valor": 10000},
        {"caminho": "achados[A1].evidencia.mediana_ms", "valor": 28.0},
        {"caminho": "script.linhas.57", "valor": "df.repartition(10000).write.format(\"delta\").save(destino)"}
      ],
      "achado_relacionado": "A1",
      "indicacao": "Pode indicar particoes demais para o volume do stage; a linha 57 do script faz repartition antes da escrita e esta possivelmente relacionada. Sugere investigar o numero de particoes usado ali.",
      "prioridade": "Alta"
    }
  ],
  "recomendacoes": [
    {
      "id": "R1",
      "tipo": "investigar",
      "texto": "Verificar se o repartition da linha 57 do script e o que gera as 10000 tasks do stage 92.0.",
      "sustentada_por": ["G1"]
    }
  ],
  "dados_insuficientes": ["GC Time Ratio indisponivel no log de driver em nivel INFO."]
}
```