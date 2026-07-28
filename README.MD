Primeiro commit (27/07) iniciando o repositório do desafio

Escopo:
 
Objetivo
 
Desenvolver uma solução que, a partir do log stderr de uma execução Spark no EMR (plano lógico, execução por executor, uso de memória, RDDs, estágios e tasks), produza um relatório técnico estruturado com classificação da execução, métricas de desempenho, gargalos identificados, recomendações de otimização e um score de qualidade — usando um assistente de IA como camada de raciocínio sobre dados já estruturados, nunca sobre o log bruto.
 
 
Contexto
 
O stderr é o log gerado pelo próprio engine Spark: plano lógico, execução por executor, uso de memória, RDDs, ciclo de vida de estágios e tasks, exceptions e status final da aplicação.
 
É nele que vivem os sinais de performance — mas também é o log mais verboso e mais difícil de tratar:
 
	1.	Pode chegar a volumes grandes em execuções longas;
	2.	Contém eventos multi-linha (stack traces, mensagens quebradas);
	3.	Se vier via log aggregation do YARN, mistura os logs de múltiplos executores/containers em um único arquivo;
 
 
O candidato deve documentar explicitamente as premissas assumidas sobre versão e verbosidade do log utilizado.
 
 
Objetivo técnico
 
A partir do stderr, utilizando investigação técnica e IA Generativa, o candidato deve:
 
	•	extrair e estruturar eventos e métricas relevantes;
	•	segmentar corretamente por executor/container, quando aplicável;
	•	calcular métricas derivadas de performance;
	•	usar um assistente de IA para interpretar o resumo estruturado e gerar classificação, gargalos e recomendações;
	•	calcular um score de qualidade de forma determinística;
	•	validar a própria análise antes de considerá-la pronta;
	•	tratar dados potencialmente sensíveis antes de qualquer envio ao assistente.
 
 
Escopo do desafio
 
 
Etapa 1 — Entendimento do problema
 
Mapear a estrutura do stderr (formato, eventos típicos, variações);
documentar as premissas assumidas (versão Spark/EMR, nível de verbosidade, se o log é single-container ou agregado);
identificar o universo de eventos relevantes: início/fim de estágio, duração de tasks, GC, shuffle, spill, exceptions, status final.
 
 
Etapa 2 — Parsing e estruturação
 
extração estruturada de eventos (regex, parser dedicado, ou lib de log parsing);
tratamento de eventos multi-linha (stack traces, mensagens quebradas);
segmentação por executor/container quando o log vier agregado;
 
antes de qualquer envio ao assistente: mascarar/redigir dados potencialmente sensíveis;
projeto para escalar: não assumir que o arquivo inteiro cabe confortavelmente em memória (streaming/chunking quando fizer sentido).
 
 
Etapa 3 — Agregação e feature engineering
 
Métricas mínimas de referência:
duração total e por estágio;
GC time ratio;
shuffle read/write;
spill (memória/disco);
skew (razão entre task mais lenta e mediana, por estágio);
executor lost / task retries / stage failures.
 
 
Etapa 4 — Uso de IA Generativa como camada de raciocínio
 
O assistente recebe apenas o resumo estruturado e mascarado, nunca o log bruto;
gera classificação, gargalos priorizados (com evidência) e recomendações específicas;
O candidato deve documentar pelo menos um caso em que discordou, corrigiu ou descartou uma sugestão do assistente — evidência de senso crítico.
 
Enviar o log bruto (ou grande parte dele, mascarado ou não) diretamente ao assistente será contabilizado como não entrega.
 
 
Etapa 5 — Scoring
 
 
Score de qualidade (0–100) com metodologia determinística e documentada.
 
 
Etapa 6 — Validação da solução
 
 
Comparar a leitura manual do candidato sobre os 3 logs de referência com o output do pipeline, reportando divergências;
medir precisão/cobertura da extração automática frente a uma amostra anotada manualmente;
na apresentação, testar contra um log não visto previamente.
 
 
Etapa 7 Entregáveis
 
 
Código-fonte organizado em módulos (repositório Git).
README.md com: como executar; 
premissas de versão/verbosidade assumidas; 
decisões de arquitetura; 
justificativa das métricas; 
metodologia do score; 
resultado da validação; casos em que o assistente foi corrigido/descartado.
Relatório gerado para os 3 logs de referência + o log holdout.