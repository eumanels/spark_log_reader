from __future__ import annotations
import json
import re

from .modelos import AnaliseIA
from .verificacao import conferir_analise, conferir_classificacao

HEDGES = ("pode", "podem", "sugere", "indicio", "indício", "possivel", "possível", "possivelmente",
          "investigar", "hipotese", "hipótese", "provavel", "provável", "eventual")

TERMOS_TECNICOS = (
    "broadcast", "cache", "persist", "repartition", "coalesce", "salting", "skewjoin",
    "spark.sql.", "spark.executor", "spark.driver", "spark.dynamicallocation", "memória", "memoria",
    "memory", "overhead", "parallelismfirst", "minpartitionsize", "advisorypartition", "optimize",
    "z-order", "zorder", "gc ", "garbage", "kryo", "speculation",
)

_NUMERO = re.compile(r"(?<![\w.])\d+(?:[.,]\d+)?(?![\w])")


def _numeros_do_resumo(resumo: dict) -> set[str]:
    texto = json.dumps(resumo, ensure_ascii=False)
    numeros = set()
    for n in _NUMERO.findall(texto):
        numeros.add(n)
        try:
            numeros.add(f"{float(n):g}")
        except ValueError:
            pass
    return numeros


def _numeros_sem_base(texto: str, base: set[str]) -> list[str]:
    faltando = []
    for n in _NUMERO.findall(texto):
        normal = n.replace(",", ".")
        try:
            g = f"{float(normal):g}"
        except ValueError:
            g = normal
        if n not in base and normal not in base and g not in base and float(normal) > 10:
            faltando.append(n)
    return faltando


def revisar(analise: AnaliseIA, resumo: dict) -> list[dict]:
    candidatos: list[dict] = []

    def add(tipo, regra, onde, disse, detalhe):
        candidatos.append({"tipo": tipo, "regra_do_prompt": regra, "onde": onde,
                           "o_que_a_ia_disse": disse, "detalhe": detalhe})

    # CLASSIFICACAO
    c = conferir_classificacao(analise, resumo)
    if not c["confere"]:
        add("CLASSIFICACAO", "Classificacao", "classificacao", c["obtida"],
            f"O criterio do prompt aplicado ao resumo da '{c['esperada']}'.")

    # EVIDENCIA
    conf = conferir_analise(analise, resumo)
    for onde, lista in [("classificacao", conf["classificacao"]), *conf["gargalos"].items()]:
        for ev in lista:
            if ev["status"] != "confere":
                add("EVIDENCIA", "1 e 2", onde, f"{ev['caminho']} = {ev['citado']}",
                    "Caminho nao existe no resumo." if ev["status"] == "inexistente"
                    else f"No resumo o valor e {ev['real']}.")

    # NUMERO_SEM_BASE
    base = _numeros_do_resumo(resumo)
    textos = [("classificacao", analise.justificativa_classificacao)]
    textos += [(g.id, f"{g.titulo} {g.indicacao}") for g in analise.gargalos]
    textos += [(r.id, r.texto) for r in analise.recomendacoes]
    for onde, texto in textos:
        faltando = _numeros_sem_base(texto, base)
        if faltando:
            add("NUMERO_SEM_BASE", "1 e 2", onde, texto,
                f"Numero(s) {', '.join(faltando)} nao aparecem no resumo (inventado ou arredondado?).")

    # AFIRMACAO
    for g in analise.gargalos:
        if not any(h in g.indicacao.lower() for h in HEDGES):
            add("AFIRMACAO", "3", g.id, g.indicacao, "Indicacao escrita como fato, sem linguagem de hipotese.")

    # TERMO_SEM_BASE
    texto_resumo = json.dumps(resumo, ensure_ascii=False).lower()
    for r in analise.recomendacoes:
        if r.tipo != "otimizar":
            continue
        sem_base = [t.strip() for t in TERMOS_TECNICOS if t in r.texto.lower() and t not in texto_resumo]
        if sem_base:
            add("TERMO_SEM_BASE", "5", r.id, r.texto,
                f"Recomenda {', '.join(sem_base)}, que o resumo nao menciona: a otimizacao tem evidencia?")

    # SO_INFO
    sev = {a["id"]: a.get("severidade") for a in resumo.get("achados", [])}
    for g in analise.gargalos:
        if g.achado_relacionado and sev.get(g.achado_relacionado) == "INFO":
            outros = [e for e in g.evidencias if not e.caminho.startswith(f"achados[{g.achado_relacionado}]")]
            if not outros:
                add("SO_INFO", "7", g.id, g.titulo,
                    f"Apoiado so no achado INFO {g.achado_relacionado}, sem outra metrica que reforce.")

    # OMISSAO_ALTA
    cobertos = {g.achado_relacionado for g in analise.gargalos}
    texto_ia = json.dumps(analise.model_dump(mode="json"), ensure_ascii=False)
    for a in resumo.get("achados", []):
        if a.get("severidade") == "ALTA" and a["id"] not in cobertos and f"[{a['id']}]" not in texto_ia:
            add("OMISSAO_ALTA", "-", a["id"], "(nao mencionado)",
                f"Achado ALTA '{a.get('regra')}' nao virou gargalo nem foi citado.")

    # SEM_LIMITACOES
    if resumo.get("metricas_nao_disponiveis") and not analise.dados_insuficientes:
        add("SEM_LIMITACOES", "6", "dados_insuficientes", "[]",
            "O resumo lista metricas indisponiveis, mas a IA nao registrou limitacoes.")

    return candidatos


def rascunho_markdown(candidatos: list[dict], log_nome: str, meta_ia: dict) -> str:
    md = [f"# Revisão da análise da IA — {log_nome}",
          f"_Prompt {meta_ia.get('prompt_versao')} · {meta_ia.get('provedor')} · {meta_ia.get('gerado_em')}_",
          "",
          "Candidatos encontrados automaticamente. Para cada um, decida e explique. "
          "Os que forem casos reais de discordância vão para `docs/casos_ia.md`.",
          ""]
    if not candidatos:
        md.append("Nenhum candidato automático. Revise manualmente as recomendações e as prioridades.")
    for i, c in enumerate(candidatos, start=1):
        md += [
            f"## Candidato {i} — {c['tipo']} ({c['onde']})",
            f"- **Regra do prompt:** {c['regra_do_prompt']}",
            f"- **O que a IA disse:** {c['o_que_a_ia_disse']}",
            f"- **O que a verificação encontrou:** {c['detalhe']}",
            "- **Minha análise:** _(preencha: a IA errou? a verificação errou? por quê?)_",
            "- **Decisão:** [ ] aceito  [ ] corrigido  [ ] descartado",
            "- **O que mudou (prompt, regra ou código), se algo mudou:** _(preencha)_",
            "",
        ]
    return "\n".join(md)