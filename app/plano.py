"""Plano de execucao do Spark (stdout) cruzado com o log (stderr) e o script.

O stdout de um job que chama ``df.explain()`` / ``df.explain(True)`` traz o
plano fisico em arvore ("== Physical Plan =="). Ele responde O QUE o Spark
executa (joins e estrategia, shuffles e chaves, agregacoes, leituras); o log
responde QUANTO custou (stages, tasks, tempo, skew); o script responde ONDE
mudar. Este modulo extrai o plano e liga as tres fontes de forma
DETERMINISTICA, declarando o grau de cada ligacao.

O que vai para a IA e estrutura, nao texto do plano: nomes de operadores,
nomes de colunas-chave, contagens. Literais de filtros (valores de negocio),
caminhos S3 e listas completas de colunas NAO sao enviados.

Formatos suportados: arvore de explain(), explain(True) e explain("extended").
explain("formatted") ainda nao e lido (vira aviso em ``fonte.avisos``).
"""
from __future__ import annotations
import ast
import hashlib
import re
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from .padroes import FORMATO_TS
from .sanitizador import Sanitizador

PLANO_VERSION = "1.1"
METODOS_LEITURA = {"load", "table", "parquet", "csv", "json", "orc", "forPath", "forName"}
METADADOS = {"columns", "schema", "dtypes"}
MIN_PREFIXO_TABELA = 8        # nome de tabela truncado pelo Spark mais curto que isso nao e casado

MAX_MARCOS = 40
MAX_CHARS_MARCO = 160
MAX_NOS = 120                 # nos relevantes enviados por plano
JANELA_COERENCIA_S = 600      # stdout x stderr: tolerancia para considerar a mesma execucao

# Operadores que entram em ``nos`` (os demais - Project, Filter, Sort, ColumnarToRow...
# - so contam em ``operadores`` e na assinatura das subarvores).
RELEVANTES = {
    "Exchange", "BroadcastExchange", "ShuffleExchange",
    "SortMergeJoin", "BroadcastHashJoin", "ShuffledHashJoin", "BroadcastNestedLoopJoin",
    "CartesianProduct",
    "HashAggregate", "SortAggregate", "ObjectHashAggregate",
    "Window", "WindowGroupLimit", "Expand", "Generate", "Union",
    "FileScan", "Scan", "InMemoryTableScan", "InMemoryRelation",
    "ReusedExchange", "AQEShuffleRead", "CustomShuffleReader",
}

_INICIO_PLANO = re.compile(r"^== Physical Plan ==\s*$")
_CABECALHO_SECAO = re.compile(r"^== .+ ==\s*$")
_TS_MARCO = re.compile(r"^(?P<ts>\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:[.,]\d+)?(?:[+-]\d{2}:?\d{2})?)\s*-?\s*(?P<msg>.*)$")
_ID_COLUNA = re.compile(r"#\d+L?")
_NOME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_FUNCOES_AGG = re.compile(r"\b(?:partial_|merge_)?(count|sum|avg|min|max|first|last|collect_list|collect_set|"
                          r"approx_count_distinct|percentile_approx|stddev\w*|variance)\(")
_PALAVRAS_EXPR = {"None", "true", "false", "null", "ASC", "DESC", "NULLS", "FIRST", "LAST", "AND", "OR",
                  "NOT", "CASE", "WHEN", "THEN", "ELSE", "END", "IN", "cast", "as", "string", "date",
                  "bigint", "int", "trim", "isnull", "isnotnull", "coalesce", "year", "month", "upper",
                  "lower", "split", "concat", "substring", "Some", "GMT", "TimestampType", "gettimestamp"}


# ============================================================== leitura do stdout
def _limpar(linha: str) -> str:
    return linha.rstrip("\r\n")


def _separar(linhas: list[str]) -> tuple[list[list[str]], list[tuple[int, str]], list[str]]:
    """Devolve (blocos de plano fisico, linhas fora de plano com indice, avisos)."""
    planos, fora, avisos = [], [], []
    i, n = 0, len(linhas)
    while i < n:
        linha = linhas[i]
        if _INICIO_PLANO.match(linha.strip()):
            bloco, i = [], i + 1
            while i < n:
                l = linhas[i]
                if not l.strip() or _CABECALHO_SECAO.match(l.strip()) or _TS_MARCO.match(l):
                    break
                bloco.append(l)
                i += 1
            if bloco and re.match(r"^\(\d+\)|^\* ", bloco[0].strip()):
                avisos.append("Plano em explain('formatted') nao suportado; use explain(True).")
            elif bloco:
                planos.append(bloco)
            continue
        if _CABECALHO_SECAO.match(linha.strip()):
            # pula as secoes logicas (Parsed/Analyzed/Optimized): nao sao enviadas
            i += 1
            while i < n and linhas[i].strip() and not _CABECALHO_SECAO.match(linhas[i].strip()) \
                    and not _TS_MARCO.match(linhas[i]):
                i += 1
            continue
        if linha.strip():
            fora.append((i, linha))
        i += 1
    return planos, fora, avisos


def _arvore(bloco: list[str]) -> list[dict]:
    """Linhas do plano -> lista de nos com profundidade, pai e texto."""
    nos: list[dict] = []
    pilha: list[tuple[int, int]] = []          # (profundidade, indice do no)
    for linha in bloco:
        m = re.search(r"[A-Za-z*(]", linha)
        if not m:
            continue
        prof = m.start()
        texto = linha[prof:].strip()
        while pilha and pilha[-1][0] >= prof:
            pilha.pop()
        pai = pilha[-1][1] if pilha else None
        nos.append({"prof": prof, "pai": pai, "texto": texto})
        pilha.append((prof, len(nos) - 1))
    for idx, no in enumerate(nos):
        no["filhos"] = [j for j, o in enumerate(nos) if o["pai"] == idx]
    return nos


def _operador(texto: str) -> str:
    m = re.match(r"\*?\(?\d*\)?\s*([A-Za-z]+)", texto)
    op = m.group(1) if m else texto.split(" ", 1)[0]
    if op == "Scan" and "ExistingRDD" in texto:
        return "ScanExistingRDD"
    return op


def _colchetes(texto: str, inicio: int) -> tuple[str, int]:
    """Conteudo do [...] que comeca em ``inicio`` (respeita aninhamento)."""
    prof, j = 0, inicio
    while j < len(texto):
        c = texto[j]
        if c in "[(":
            prof += 1
        elif c in "])":
            prof -= 1
            if prof == 0:
                return texto[inicio + 1:j], j + 1
        j += 1
    return texto[inicio + 1:], len(texto)


def _dividir_topo(conteudo: str) -> list[str]:
    """'a, f(b, c), d' -> ['a', 'f(b, c)', 'd']."""
    partes, prof, atual = [], 0, ""
    for c in conteudo:
        if c in "([":
            prof += 1
        elif c in ")]":
            prof -= 1
        if c == "," and prof == 0:
            partes.append(atual.strip())
            atual = ""
        else:
            atual += c
    if atual.strip():
        partes.append(atual.strip())
    return partes


def _chave(expr: str) -> str:
    """Normaliza uma chave: 'COL#12' -> 'COL'; expressao -> 'expr(COL1, COL2)'. Sem literais."""
    expr = re.sub(r"'[^']*'", "", expr)
    simples = re.fullmatch(r"([A-Za-z_][A-Za-z0-9_.]*)#\d+L?", expr.strip())
    if simples:
        return simples.group(1).split(".")[-1]
    nomes = []
    for m in re.finditer(r"([A-Za-z_][A-Za-z0-9_.]*)#\d+", expr):
        nome = m.group(1).split(".")[-1]
        if nome not in nomes:
            nomes.append(nome)
    if not nomes:
        nomes = [n for n in _NOME.findall(_ID_COLUNA.sub("", expr))
                 if n not in _PALAVRAS_EXPR and n.isupper() and len(n) > 2][:4]
    return f"expr({', '.join(nomes)})" if nomes else "expr()"


def _lista_chaves(texto: str, rotulo: str) -> list[str]:
    i = texto.find(rotulo)
    if i < 0:
        return []
    conteudo, _ = _colchetes(texto, i + len(rotulo) - 1)
    return [_chave(p) for p in _dividir_topo(conteudo) if p]


def _atributos(op: str, texto: str) -> dict:
    a: dict = {}
    if op in ("Exchange", "ShuffleExchange"):
        m = re.match(r"\w+ (\w+)\((.*)\)", texto)
        if "SinglePartition" in texto:
            a["particionamento"] = "single"
        elif m:
            a["particionamento"] = m.group(1).replace("partitioning", "")
            partes = _dividir_topo(m.group(2))
            if partes and partes[-1].isdigit():
                a["particoes"] = int(partes[-1])
                partes = partes[:-1]
            a["chaves"] = [_chave(p) for p in partes]
    elif op in ("SortMergeJoin", "BroadcastHashJoin", "ShuffledHashJoin"):
        i = texto.find("[")
        esq, j = _colchetes(texto, i)
        k = texto.find("[", j)
        dir_, j2 = _colchetes(texto, k)
        resto = [p.strip() for p in _dividir_topo(texto[j2:].lstrip(", "))]
        a["chaves_esq"] = [_chave(p) for p in _dividir_topo(esq)]
        a["chaves_dir"] = [_chave(p) for p in _dividir_topo(dir_)]
        a["tipo_join"] = resto[0] if resto else None
        if op == "BroadcastHashJoin" and len(resto) > 1:
            a["lado_broadcast"] = {"BuildRight": "direito", "BuildLeft": "esquerdo"}.get(resto[1])
        a["condicao_extra"] = any(r for r in resto[1:] if r not in ("BuildRight", "BuildLeft", "false", "true"))
        a["chave_com_funcao"] = any(c.startswith("expr(") for c in a["chaves_esq"] + a["chaves_dir"])
    elif op == "BroadcastNestedLoopJoin":
        a["tipo_join"] = (re.findall(r"\b(Inner|LeftOuter|RightOuter|FullOuter|LeftSemi|LeftAnti|Cross)\b", texto)
                          or [None])[0]
    elif op in ("HashAggregate", "SortAggregate", "ObjectHashAggregate"):
        a["chaves"] = _lista_chaves(texto, "keys=[") or _lista_chaves(texto, "key=[")
        funcoes = texto[texto.find("functions=["):] if "functions=[" in texto else ""
        funcoes = funcoes[:funcoes.find("], output=")] if "], output=" in funcoes else funcoes
        a["funcoes"] = sorted({m.group(1) for m in _FUNCOES_AGG.finditer(funcoes)})
        a["parcial"] = "partial_" in funcoes
        a["distinct"] = "distinct" in funcoes
    elif op == "Window":
        conteudos = []
        j = 0
        while True:
            i = texto.find("[", j)
            if i < 0:
                break
            c, j = _colchetes(texto, i)
            conteudos.append(c)
        if len(conteudos) >= 3:
            a["particao"] = [_chave(p) for p in _dividir_topo(conteudos[1])]
            a["ordem"] = [_chave(p) for p in _dividir_topo(conteudos[2])]
        a["funcoes"] = sorted(set(re.findall(r"\b(row_number|rank|dense_rank|lag|lead|sum|count|max|min|"
                                             r"first|last|ntile|percent_rank|cume_dist)\(", conteudos[0] if conteudos else "")))
        a["sem_particao"] = not a.get("particao")
    elif op == "Expand":
        corpo = texto[texto.find("[") + 1:] if "[" in texto else ""
        a["projecoes"] = max(1, len(re.findall(r"\[(?!\[)", corpo)))
    elif op == "FileScan":
        m = re.match(r"FileScan (\w+) \[([^\]]*)\]", texto)
        if m:
            a["formato"] = m.group(1)
            a["colunas"] = len([c for c in m.group(2).split(",") if c.strip()])
        loc = re.search(r"Location: [^\[]*\[([^\]]*)\]?", texto)
        if loc:
            caminho = loc.group(1).split(",")[0].strip()
            seg = [s for s in re.split(r"[/\\]", caminho) if s]
            nome = seg[-1] if seg else None
            if nome:
                a["tabela"] = nome
                a["tabela_truncada"] = nome.endswith("...")
        a["delta"] = "Delta" in texto
        pf = re.search(r"PartitionFilters: \[([^\]]*)\]", texto)
        a["filtros_particao"] = len(_dividir_topo(pf.group(1))) if pf and pf.group(1).strip() else 0
        pu = re.search(r"PushedFilters: \[([^\]]*)", texto)
        a["filtros_empurrados"] = len(_dividir_topo(pu.group(1))) if pu and pu.group(1).strip() else 0
        a["filtro_de_dados"] = bool(re.search(r"DataFilters: \[[^\]\s]", texto))
    elif op == "BroadcastExchange":
        a["modo"] = "hashed" if "HashedRelation" in texto else "identity"
    elif op == "WindowGroupLimit":
        a["fase"] = "Final" if texto.rstrip().endswith("Final") else "Partial"
    return a


def _assinatura(op: str, a: dict) -> str:
    """Assinatura ESTRUTURAL do no: operador + chaves (sem colunas projetadas nem filtros)."""
    partes = [op]
    for k in ("particionamento", "chaves", "chaves_esq", "chaves_dir", "tipo_join", "tabela", "particao"):
        if a.get(k):
            partes.append(f"{k}={a[k]}")
    return "|".join(partes)


def _analisar_plano(bloco: list[str], rotulo: str | None) -> dict:
    nos = _arvore(bloco)
    for no in nos:
        no["op"] = _operador(no["texto"])
        no["attr"] = _atributos(no["op"], no["texto"])
        no["sig"] = _assinatura(no["op"], no["attr"])
    raiz = nos[0]["texto"] if nos else ""
    final = None
    m = re.search(r"isFinalPlan=(true|false)", raiz)
    if m:
        final = m.group(1) == "true"

    # ids P1.. so para os relevantes; pai = ancestral relevante mais proximo
    ids: dict[int, str] = {}
    for idx, no in enumerate(nos):
        if no["op"] in RELEVANTES or no["op"] == "ScanExistingRDD":
            ids[idx] = f"P{len(ids) + 1}"

    def pai_relevante(idx):
        p = nos[idx]["pai"]
        while p is not None and p not in ids:
            p = nos[p]["pai"]
        return ids.get(p)

    saida_nos = []
    for idx, pid in ids.items():
        no = nos[idx]
        saida_nos.append({"id": pid, "op": no["op"], "pai": pai_relevante(idx),
                          "nivel": no["prof"] // 3, **no["attr"]})
    omitidos = max(0, len(saida_nos) - MAX_NOS)
    return {
        "rotulo": rotulo,
        "plano_final_aqe": final,
        "operadores": dict(Counter(n["op"] for n in nos).most_common()),
        "particoes_shuffle": sorted({n["attr"]["particoes"] for n in nos if n["attr"].get("particoes")}),
        "nos": saida_nos[:MAX_NOS],
        "nos_omitidos": omitidos,
        "_interno": {"nos": nos, "ids": ids},
    }


# ===================================================================== sinais
def _cadeia_repetida(nos: list[dict], ids: dict, indices: list[int]) -> list[int]:
    """Sobe em paralelo a partir de k scans enquanto a assinatura estrutural for igual.

    Devolve, para o PRIMEIRO scan, os indices da cadeia comum (do scan ate o topo comum)."""
    atuais = list(indices)
    cadeia = [atuais[0]]
    while True:
        pais = [nos[i]["pai"] for i in atuais]
        if any(p is None for p in pais) or len(set(pais)) < len(pais):
            break                       # chegou na raiz ou os ramos se juntaram
        if len({nos[p]["sig"] for p in pais}) != 1:
            break
        atuais = pais
        cadeia.append(atuais[0])
    return cadeia


def _sinais(plano: dict, n_plano: int) -> list[dict]:
    nos, ids = plano["_interno"]["nos"], plano["_interno"]["ids"]
    sinais: list[dict] = []

    def add(regra, **ev):
        sinais.append({"id": f"S{n_plano}.{len(sinais) + 1}", "plano": n_plano, "regra": regra, **ev})

    # 1. mesma tabela lida varias vezes no mesmo plano (reprocessamento)
    por_tabela: dict[str, list[int]] = defaultdict(list)
    for idx, no in enumerate(nos):
        if no["op"] == "FileScan" and no["attr"].get("tabela"):
            por_tabela[no["attr"]["tabela"]].append(idx)
    for tabela, idxs in por_tabela.items():
        if len(idxs) < 2:
            continue
        cadeia = _cadeia_repetida(nos, ids, idxs)
        ops = [nos[i]["op"] for i in cadeia]
        add("tabela_lida_varias_vezes", tabela=tabela, leituras=len(idxs),
            nos_scan=[ids[i] for i in idxs if i in ids],
            operacoes_repetidas=[o for o in ops if o not in ("Project", "Filter", "ColumnarToRow")],
            shuffles_repetidos=sum(1 for i in cadeia if nos[i]["op"] in ("Exchange", "ShuffleExchange")),
            chaves_do_shuffle_repetido=[nos[i]["attr"].get("chaves") for i in cadeia
                                        if nos[i]["op"] in ("Exchange", "ShuffleExchange")][:2],
            tabela_truncada=nos[idxs[0]]["attr"].get("tabela_truncada", False))

    # 2. count(distinct) multiplos -> Expand (multiplica as linhas)
    expands = [i for i, n in enumerate(nos) if n["op"] == "Expand"]
    if expands:
        add("expand_count_distinct", ocorrencias=len(expands), nos=[ids[i] for i in expands],
            projecoes=[nos[i]["attr"].get("projecoes") for i in expands])

    # 3. SortAggregate (agregacao por ordenacao; ex.: first()/dropDuplicates sobre texto)
    sorts = [i for i, n in enumerate(nos) if n["op"] == "SortAggregate"]
    if sorts:
        chaves = []
        for i in sorts:
            c = nos[i]["attr"].get("chaves")
            if c and c not in chaves:
                chaves.append(c)
        add("sort_aggregate", ocorrencias=len(sorts), nos=[ids[i] for i in sorts][:12],
            chaves_distintas=chaves[:6],
            funcoes=sorted({f for i in sorts for f in nos[i]["attr"].get("funcoes", [])}))

    # 4. broadcast de DataFrame criado em memoria (createDataFrame / parallelize)
    rdd = [i for i, n in enumerate(nos) if n["op"] == "ScanExistingRDD"]
    if rdd:
        add("broadcast_de_dataframe_em_memoria", ocorrencias=len(rdd), nos=[ids[i] for i in rdd])

    # 5. leituras sem filtro de particao (le a tabela inteira)
    sem_pf = [(i, n) for i, n in enumerate(nos) if n["op"] == "FileScan" and n["attr"].get("filtros_particao") == 0]
    if sem_pf:
        add("leitura_sem_filtro_de_particao",
            tabelas=sorted({n["attr"].get("tabela") or "?" for _, n in sem_pf}),
            com_filtro_de_dados=sorted({n["attr"].get("tabela") or "?" for _, n in sem_pf
                                        if n["attr"].get("filtro_de_dados")}))

    # 6. joins e estrategia
    joins = [(i, n) for i, n in enumerate(nos) if n["op"] in ("SortMergeJoin", "BroadcastHashJoin",
                                                              "ShuffledHashJoin", "BroadcastNestedLoopJoin",
                                                              "CartesianProduct")]
    if joins:
        add("joins", total=len(joins), por_estrategia=dict(Counter(n["op"] for _, n in joins)),
            sort_merge=[{"no": ids[i], "chaves": n["attr"].get("chaves_esq"), "tipo": n["attr"].get("tipo_join")}
                        for i, n in joins if n["op"] == "SortMergeJoin"],
            com_funcao_na_chave=[ids[i] for i, n in joins if n["attr"].get("chave_com_funcao")],
            nested_loop_ou_cartesiano=[ids[i] for i, n in joins
                                       if n["op"] in ("BroadcastNestedLoopJoin", "CartesianProduct")])

    # 7. janelas
    janelas = [(i, n) for i, n in enumerate(nos) if n["op"] == "Window"]
    if janelas:
        add("janelas", total=len(janelas),
            sem_particao=[ids[i] for i, n in janelas if n["attr"].get("sem_particao")],
            particoes=[n["attr"].get("particao") for _, n in janelas][:4],
            com_group_limit=any(n["op"] == "WindowGroupLimit" for n in nos))

    # 8. shuffles
    trocas = [n for n in nos if n["op"] in ("Exchange", "ShuffleExchange")]
    if trocas:
        add("shuffles", total=len(trocas), particoes_configuradas=plano["particoes_shuffle"],
            single_partition=sum(1 for n in trocas if n["attr"].get("particionamento") == "single"))
    return sinais


# ============================================================== cruzamento log
def _scans_do_log(eventos) -> list[dict]:
    """Cada scan do log com o stage que o executou (proximo stage com tasks == arquivos)."""
    scans, pendentes = [], []
    for e in eventos:
        if e.event_type == "scan_bin_packing":
            s = {"ts": e.ts, "arquivos": int(e.data["num_files"]), "stage": None}
            scans.append(s)
            pendentes.append(s)
        elif e.event_type == "stage_tasks_submitted" and pendentes:
            n = int(e.data["num_tasks"])
            for s in pendentes:
                if s["arquivos"] == n:
                    s["stage"] = e.data["stage_id"]
                    pendentes.remove(s)
                    break
            if len(pendentes) > 6:
                pendentes.pop(0)
    return scans


def _cruzar_log(sinais: list[dict], eventos, relatorio: dict) -> list[dict]:
    stages = {s["stage_id"]: s for s in relatorio.get("stages", [])
              if s.get("status_tentativa") in ("concluida", None)}
    scans = _scans_do_log(eventos)
    por_arquivos: dict[int, list[dict]] = defaultdict(list)
    for s in scans:
        por_arquivos[s["arquivos"]].append(s)
    saida = []

    def resumo_stages(ids_):
        st = [stages[i] for i in ids_ if i in stages]
        return {
            "stages": [s["stage_key"] for s in st],
            "duracao_stages_s": round(sum(s.get("duration_s") or 0 for s in st), 3),
            "tempo_total_task_s": round(sum(s["task_duration_ms"]["total"] for s in st) / 1000, 1),
            "skew_max": max((s.get("skew_ratio") or 0 for s in st), default=None),
        }

    for s in sinais:
        if s["regra"] == "tabela_lida_varias_vezes":
            k = s["leituras"]
            grupos = [a for a, lst in por_arquivos.items() if len(lst) == k]
            if len(grupos) == 1:
                lst = por_arquivos[grupos[0]]
                saida.append({"sinal": s["id"], "ligacao_plano_log": "multiplicidade",
                              "explicacao": f"o plano le {s['tabela']} {k}x e o log tem exatamente {k} scans "
                                            f"de {grupos[0]} arquivos",
                              "log": {"scans": k, "arquivos_por_scan": grupos[0],
                                      "horarios": [x["ts"] for x in lst],
                                      **resumo_stages([x["stage"] for x in lst if x["stage"]])}})
            else:
                saida.append({"sinal": s["id"], "ligacao_plano_log": "sem_correspondencia_unica",
                              "explicacao": f"nenhum (ou mais de um) grupo de exatamente {k} scans com o mesmo "
                                            "numero de arquivos no log",
                              "log": {"grupos_candidatos": sorted(grupos)}})
        elif s["regra"] == "shuffles" and s.get("particoes_configuradas"):
            n = s["particoes_configuradas"][-1]
            com_n = [x["stage_key"] for x in stages.values() if x.get("num_tasks_submetidas") == n
                     and not (x.get("origem") or {}).get("acao", "").startswith("$anonfun$withThreadLocalCaptured")]
            saida.append({"sinal": s["id"], "ligacao_plano_log": "valor",
                          "explicacao": f"stages do log com exatamente {n} tasks (o numero de particoes do "
                                        "plano, sem coalescencia do AQE)",
                          "log": {"particoes_do_plano": n, "stages_com_esse_numero_de_tasks": com_n[:12],
                                  "total": len(com_n), **resumo_stages([k.split(".")[0] for k in com_n])}})
        elif s["regra"] == "broadcast_de_dataframe_em_memoria":
            bc = [x for x in stages.values() if (x.get("origem") or {}).get("acao", "").startswith(
                "$anonfun$withThreadLocalCaptured") and x.get("tipo_stage") == "ResultStage"]
            if bc:
                saida.append({"sinal": s["id"], "ligacao_plano_log": "tipo",
                              "explicacao": "ResultStages disparados por withThreadLocalCaptured (thread de "
                                            "broadcast) sao compativeis com a materializacao de broadcasts; "
                                            "podem incluir outros jobs assincronos",
                              "log": {"tasks_por_stage": [x["num_tasks"] for x in bc],
                                      **resumo_stages([x["stage_id"] for x in bc])}})
    return saida


# =========================================================== cruzamento script
class _Script:
    """Indice do script via ast: atribuicoes, usos e chamadas, com linhas."""

    def __init__(self, fonte: str):
        self.fonte = fonte
        self.arvore = ast.parse(fonte)
        self.atrib: dict[str, list[tuple[int, set[str]]]] = defaultdict(list)
        self.chamadas: list[tuple[str, int, str, str]] = []   # (metodo, linha, texto dos args, objeto)
        self.chamadas_detalhe: list[dict] = []
        self.constantes: list[tuple[int, str]] = []
        self.leituras: list[tuple[str, int, set[str], list[str]]] = []  # (alvo, linha, nomes, literais)
        for no in ast.walk(self.arvore):
            if isinstance(no, ast.Assign):
                # x = df.columns / df.schema nao e reuso do DataFrame (so metadado)
                if isinstance(no.value, ast.Attribute) and no.value.attr in METADADOS:
                    continue
                usados = {n.id for n in ast.walk(no.value) if isinstance(n, ast.Name)}
                alvos = [a.id for a in no.targets if isinstance(a, ast.Name)]
                for alvo in alvos:
                    self.atrib[alvo].append((no.lineno, usados))
                for c in ast.walk(no.value):
                    if isinstance(c, ast.Call) and isinstance(c.func, ast.Attribute) \
                            and c.func.attr in METODOS_LEITURA:
                        nomes = {n.id for a in c.args for n in ast.walk(a) if isinstance(n, ast.Name)}
                        lits = [a.value for a in c.args if isinstance(a, ast.Constant) and isinstance(a.value, str)]
                        for alvo in alvos:
                            self.leituras.append((alvo, no.lineno, nomes, lits))
            if isinstance(no, ast.Call):
                f = no.func
                nome = f.attr if isinstance(f, ast.Attribute) else (f.id if isinstance(f, ast.Name) else None)
                if nome:
                    # Em cadeias (df.a().b().join(...)) o lineno da Call e o inicio da cadeia;
                    # a linha util e a do nome do metodo.
                    linha = f.end_lineno if isinstance(f, ast.Attribute) else no.lineno
                    args = " ".join(ast.get_source_segment(fonte, a) or "" for a in
                                    list(no.args) + [k.value for k in no.keywords])
                    obj = (ast.get_source_segment(fonte, f.value) or "")[-200:] if isinstance(f, ast.Attribute) else ""
                    self.chamadas.append((nome, linha, args, obj))
                    self.chamadas_detalhe.append({"metodo": nome, "linha": linha, "metodo_de_objeto":
                                                  isinstance(f, ast.Attribute), "args": args[:60], "obj": obj})
            if isinstance(no, ast.Constant) and isinstance(no.value, str):
                self.constantes.append((no.lineno, no.value))

    def usuarios(self, var: str) -> list[tuple[str, int]]:
        return sorted({(alvo, linha) for alvo, lst in self.atrib.items() for linha, usados in lst
                       if var in usados and alvo != var}, key=lambda x: x[1])

    def cache(self, var: str) -> list[int]:
        """Linhas com cache/persist ligados a ``var`` (no proprio objeto ou na definicao de var)."""
        definicoes = {l for l, _ in self.atrib.get(var, [])}
        fim_def = {l: self._fim_atrib(l) for l in definicoes}
        linhas = []
        for nome, linha, _, obj in self.chamadas:
            if nome not in ("cache", "persist", "checkpoint", "localCheckpoint"):
                continue
            if re.search(rf"\b{re.escape(var)}\b", obj) or any(l <= linha <= f for l, f in fim_def.items()):
                linhas.append(linha)
        return sorted(set(linhas))

    def _fim_atrib(self, linha: int) -> int:
        for no in ast.walk(self.arvore):
            if isinstance(no, ast.Assign) and no.lineno == linha:
                return no.end_lineno or linha
        return linha

    def linhas_chamada(self, metodos: set[str], nos_args: list[str] | None = None,
                       no_objeto: str | None = None) -> list[int]:
        res = set()
        for nome, linha, args, obj in self.chamadas:
            if nome not in metodos:
                continue
            if nos_args and not all(re.search(rf"\b{re.escape(c)}\b", args) for c in nos_args):
                continue
            if no_objeto and no_objeto not in obj:
                continue
            res.add(linha)
        return sorted(res)


def _var_da_leitura(sc: _Script, tabela: str) -> tuple[int | None, str | None, int | None]:
    """tabela -> (linha da constante, variavel do DataFrame lido, linha da leitura)."""
    tabela = (tabela or "").replace("...", "")
    if len(tabela) < MIN_PREFIXO_TABELA:
        return None, None, None
    candidatas = [(l, v) for l, v in sc.constantes if tabela in v]
    if not candidatas:
        return None, None, None
    linha_const = candidatas[0][0]
    # leitura direta: spark.read.load("s3://.../TABELA")
    for alvo, l, _, lits in sc.leituras:
        if any(tabela in x for x in lits):
            return linha_const, alvo, l
    # leitura via variavel: caminho = "...TABELA"; df = spark.read.load(caminho)
    vars_caminho = {alvo for alvo, lst in sc.atrib.items() for l, _ in lst
                    if any(l <= lc <= l + 3 for lc, _ in candidatas)}
    for alvo, l, nomes, _ in sc.leituras:
        if nomes & vars_caminho:
            return linha_const, alvo, l
    return linha_const, None, None


def _cruzar_script(sinais: list[dict], fonte: str | None, nome_script: str | None) -> list[dict]:
    if not fonte:
        return []
    try:
        sc = _Script(fonte)
    except SyntaxError:
        return [{"regra": "script_nao_analisado", "motivo": "o script nao pode ser lido pelo ast (SyntaxError)"}]
    saida = []
    for s in sinais:
        r = s["regra"]
        if r == "tabela_lida_varias_vezes":
            l_const, var, l_leitura = _var_da_leitura(sc, s["tabela"])
            item = {"sinal": s["id"], "ligacao_plano_script": "nome" if l_const else "sem_correspondencia",
                    "linha_nome_tabela": l_const, "dataframe_lido": var, "linha_leitura": l_leitura}
            v, caminho = var, []
            while v:
                us = sc.usuarios(v)
                if len(us) == 1:
                    caminho.append(us[0][0])
                    v = us[0][0]
                    continue
                if len(us) >= 2:
                    l_def = min(l for l, _ in sc.atrib[v])
                    item.update({
                        "dataframe_reutilizado": v,
                        "linha_definicao": l_def,
                        "linha_fim_definicao": sc._fim_atrib(l_def),
                        "usado_por": [{"dataframe": a, "linha": l} for a, l in us],
                        "cache_ou_persist": sc.cache(v) or None,
                    })
                break
            if caminho:
                item["cadeia"] = caminho
            saida.append(item)
        elif r == "joins":
            chaves = sorted({c for j in s.get("sort_merge", []) for c in (j.get("chaves") or [])
                             if not c.startswith("expr(")})
            saida.append({"sinal": s["id"], "ligacao_plano_script": "nome",
                          "joins_sort_merge_por_chave": {c: sc.linhas_chamada({"join"}, nos_args=[c]) for c in chaves},
                          "linhas_join": sc.linhas_chamada({"join"}),
                          "linhas_broadcast_explicito": sc.linhas_chamada({"broadcast"})})
        elif r == "expand_count_distinct":
            saida.append({"sinal": s["id"], "ligacao_plano_script": "nome",
                          "linhas_count_distinct": sc.linhas_chamada({"countDistinct", "count_distinct"})})
        elif r == "sort_aggregate":
            item = {"sinal": s["id"], "ligacao_plano_script": "nome",
                    "linhas_drop_duplicates": sc.linhas_chamada({"dropDuplicates", "drop_duplicates"}),
                    "linhas_first": sc.linhas_chamada({"first"})}
            saida.append(item)
        elif r == "janelas":
            saida.append({"sinal": s["id"], "ligacao_plano_script": "nome",
                          "linhas_window": sc.linhas_chamada({"partitionBy"}, no_objeto="Window")
                                           or sc.linhas_chamada({"over"})})
        elif r == "broadcast_de_dataframe_em_memoria":
            saida.append({"sinal": s["id"], "ligacao_plano_script": "nome",
                          "linhas_create_dataframe": sc.linhas_chamada({"createDataFrame", "parallelize"})})
        elif r == "shuffles":
            conf = [l for nome, l, a, _ in sc.chamadas if nome == "set" and "shuffle.partitions" in a]
            saida.append({"sinal": s["id"], "ligacao_plano_script": "valor" if conf else "sem_correspondencia",
                          "linhas_conf_shuffle_partitions": conf or None,
                          "observacao": None if conf else "spark.sql.shuffle.partitions nao e definido neste "
                                                          "script (vem da sessao/cluster ou de modulo externo)"})
    return saida


# ======================================================================= marcos
def _marcos(fora: list[tuple[int, str]], sanitizador: Sanitizador) -> list[dict]:
    marcos = []
    for _, linha in fora:
        m = _TS_MARCO.match(linha.strip())
        if m:
            msg = re.sub(r"^\s*-?\s*(INFO|WARN|WARNING|ERROR|DEBUG)\s*-\s*", "", m.group("msg"))
            marcos.append({"ts": m.group("ts"), "msg": sanitizador.mask_text(msg)[:MAX_CHARS_MARCO]})
        else:
            marcos.append({"ts": None, "msg": sanitizador.mask_text(linha.strip())[:MAX_CHARS_MARCO]})
    return marcos[:MAX_MARCOS]


def _coerencia(marcos: list[dict], relatorio: dict) -> dict:
    """O stdout e da mesma execucao do stderr? Compara horarios (sem fuso)."""
    d = relatorio.get("duracao_total_execucao") or {}
    try:
        ini = datetime.strptime(d["inicio"], FORMATO_TS)
        fim = datetime.strptime(d["fim"], FORMATO_TS)
    except (KeyError, TypeError, ValueError):
        return {"status": "nao_verificado"}
    dentro = total = 0
    for m in marcos:
        if not m["ts"]:
            continue
        try:
            t = datetime.strptime(m["ts"][:19].replace("T", " "), "%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
        total += 1
        if (ini - t).total_seconds() <= JANELA_COERENCIA_S and (t - fim).total_seconds() <= JANELA_COERENCIA_S:
            dentro += 1
    if not total:
        return {"status": "nao_verificado"}
    return {"status": "ok" if dentro else "divergente", "marcos_na_janela_do_stderr": dentro,
            "marcos_com_horario": total}



# ===================================================================== fases
# Os marcos do stdout (mensagens do proprio script, com horario) dividem a execucao em
# trechos do script. Cada fase junta: duracao (stdout), jobs/stages que rodaram nela
# (stderr) e as operacoes do script entre as duas mensagens (ast).
OPERACOES_NOTAVEIS = {
    "manutencao_delta": {"vacuum", "optimize", "generate", "restoreToVersion", "executeCompaction",
                         "executeZOrderBy", "history", "detail"},
    "acao_no_driver": {"collect", "toPandas", "count", "first", "take", "head", "show", "toLocalIterator",
                       "foreach", "foreachPartition"},
    "escrita": {"save", "saveAsTable", "insertInto", "execute", "writeTo"},
    "leitura": {"load", "table", "parquet", "csv", "json", "forPath", "forName", "sql"},
}
_GRUPO_DO_METODO = {m: g for g, ms in OPERACOES_NOTAVEIS.items() for m in ms}


def _hora_marco(ts: str | None) -> datetime | None:
    """Horario local do marco; marcos com fuso explicito (+00:00) sao de outro relogio e ficam de fora."""
    if not ts or re.search(r"[+-]\d{2}:?\d{2}$", ts.strip()):
        return None
    try:
        return datetime.strptime(ts[:19].replace("T", " "), "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def _linha_da_mensagem(sc: "_Script | None", msg: str, depois_de: int) -> int | None:
    if sc is None:
        return None
    alvo = msg.strip()
    exatas = sorted(l for l, v in sc.constantes if v.strip() and v.strip() == alvo)
    contidas = sorted(l for l, v in sc.constantes if len(v.strip()) >= 10 and v.strip() in alvo)
    for lista in (exatas, contidas):
        if lista:
            return next((l for l in lista if l > depois_de), lista[0])
    return None


def _operacoes(sc: "_Script | None", de: int | None, ate: int | None) -> list[dict]:
    if sc is None:
        return []
    ops = []
    for c in sc.chamadas_detalhe:
        grupo = _GRUPO_DO_METODO.get(c["metodo"])
        if not grupo or not c["metodo_de_objeto"]:
            continue                                   # F.count(col)/first(col) sao funcoes, nao acoes
        if grupo == "acao_no_driver" and re.search(r"(groupBy|groupby|agg)\([^)]*\)\s*$", c["obj"]):
            continue                                   # groupBy(...).count() e transformacao
        if (de is None or c["linha"] > de) and (ate is None or c["linha"] < ate):
            ops.append({"metodo": c["metodo"], "linha": c["linha"], "grupo": grupo,
                        **({"args": c["args"]} if c["args"] and grupo == "manutencao_delta" else {})})
    vistos, unicos = set(), []
    for o in sorted(ops, key=lambda o: o["linha"]):
        if (o["metodo"], o["linha"]) not in vistos:
            vistos.add((o["metodo"], o["linha"]))
            unicos.append(o)
    return unicos


def _fases(marcos: list[dict], eventos, relatorio: dict, sc: "_Script | None", coerente: bool) -> list[dict]:
    slots = (relatorio.get("capacidade_cluster") or {}).get("slots")
    if not coerente:
        return []
    d = relatorio.get("duracao_total_execucao") or {}
    try:
        t_ini = datetime.strptime(d["inicio"], FORMATO_TS)
        t_fim = datetime.strptime(d["fim"], FORMATO_TS)
    except (KeyError, TypeError, ValueError):
        return []

    # pontos: inicio do log, marcos com horario local (mesmo horario -> fica o ultimo), fim do log
    pontos = [{"msg": "inicio do log (stderr)", "linha": None, "t": t_ini}]
    ultima_linha = 0
    for m in marcos:
        t = _hora_marco(m.get("ts"))
        if t is None or t < t_ini - _seg(JANELA_COERENCIA_S) or t > t_fim + _seg(JANELA_COERENCIA_S):
            continue
        linha = _linha_da_mensagem(sc, m["msg"], ultima_linha)
        if linha:
            ultima_linha = linha
        ponto = {"msg": m["msg"], "linha": linha, "t": t}
        if len(pontos) > 1 and pontos[-1]["t"] == t:
            # mesmo horario: fica a mensagem do script (com linha); entre duas do script, a ultima
            if linha is not None or pontos[-1]["linha"] is None:
                pontos[-1] = ponto
        else:
            pontos.append(ponto)
    pontos.append({"msg": "fim do log (stderr)", "linha": None, "t": t_fim})

    stages = {s["stage_id"]: s for s in relatorio.get("stages", []) if s.get("stage_attempt") == 0}
    sub_ts: dict[str, datetime] = {}
    jobs_ts: list[datetime] = []
    for e in eventos:
        try:
            t = datetime.strptime(e.ts, FORMATO_TS)
        except (TypeError, ValueError):
            continue
        if e.event_type == "stage_tasks_submitted":
            sub_ts.setdefault(e.data["stage_id"], t)
        elif e.event_type in ("job_start", "map_stage_job_start"):
            jobs_ts.append(t)

    achados_por_stage = {}
    for i, a in enumerate(relatorio.get("oportunidades", []), start=1):
        k = (a.get("evidencia") or {}).get("stage_key")
        if k:
            achados_por_stage.setdefault(k.split(".")[0], []).append(f"A{i}")

    fases = []
    for a, b in zip(pontos, pontos[1:]):
        dur = (b["t"] - a["t"]).total_seconds()
        if dur <= 0:
            continue
        ultimo = b is pontos[-1]
        dentro = [sid for sid, t in sub_ts.items() if a["t"] <= t < b["t"] or (ultimo and t == b["t"])]
        st = [stages[s] for s in dentro if s in stages]
        maior = max(st, key=lambda s: s.get("duration_s") or 0, default=None)
        fase = {
            "id": f"F{len(fases) + 1}",
            "de": a["msg"], "ate": b["msg"],
            "linhas_script": [a["linha"], b["linha"]] if (a["linha"] or b["linha"]) else None,
            "inicio": a["t"].strftime("%H:%M:%S"),
            "duracao_s": round(dur, 1),
            "jobs": sum(1 for t in jobs_ts if a["t"] <= t < b["t"] or (ultimo and t == b["t"])),
            "stages": len(st),
            "tempo_em_stages_s": round(sum(s.get("duration_s") or 0 for s in st), 3),
            "tempo_total_task_s": round(sum(s["task_duration_ms"]["total"] for s in st) / 1000, 1),
        }
        if slots:
            cap = slots * dur
            fase["ocupacao_pct"] = round(100 * fase["tempo_total_task_s"] / cap, 1)
            fase["slot_s_ocioso"] = round(max(0.0, cap - fase["tempo_total_task_s"]), 1)
        if maior:
            fase["maior_stage"] = {"stage_key": maior["stage_key"], "num_tasks": maior["num_tasks"],
                                   "duration_s": maior.get("duration_s"),
                                   "origem_acao": (maior.get("origem") or {}).get("acao")}
        achs = sorted({x for s in dentro for x in achados_por_stage.get(s, [])})
        if achs:
            fase["achados"] = achs
        if sc is not None and (a["linha"] or b["linha"]):
            fase["operacoes"] = _operacoes(sc, a["linha"], b["linha"])
        fases.append(fase)
    return fases


def _seg(n: int):
    from datetime import timedelta
    return timedelta(seconds=n)


# ================================================================ candidatas
# Recomendacoes PRONTAS, montadas sem IA, cada uma com: custo (log), mecanismo
# (plano ou operacao do script), local (linhas) e como confirmar. A IA valida,
# prioriza e escreve; nao precisa descobri-las.
def _candidata_ociosidade(fases: list[dict], relatorio: dict) -> dict | None:
    """Onde esta o tempo ocioso? So quando o log tem o achado de ocupacao baixa."""
    achado = next((f"A{i}" for i, a in enumerate(relatorio.get("oportunidades", []), start=1)
                   if a.get("regra") == "ocupacao_baixa"), None)
    com_dado = [f for f in fases if f.get("slot_s_ocioso") is not None]
    total = sum(f["slot_s_ocioso"] for f in com_dado)
    if not achado or not com_dado or total <= 0:
        return None
    pior = max(com_dado, key=lambda f: f["slot_s_ocioso"])
    pct = round(100 * pior["slot_s_ocioso"] / total, 1)
    com_jobs = [f for f in com_dado if f["jobs"]]
    slots = (relatorio.get("capacidade_cluster") or {}).get("slots")
    ocup_com_jobs = (round(100 * sum(f["tempo_total_task_s"] for f in com_jobs)
                           / (slots * sum(f["duracao_s"] for f in com_jobs)), 1)
                     if com_jobs and slots else None)
    trecho = pior.get("linhas_script")
    if pior["jobs"] == 0:
        onde_txt = (f"antes da linha {trecho[1]}" if trecho and trecho[0] is None and trecho[1]
                    else f"entre as linhas {trecho[0]} e {trecho[1]}" if trecho else "fora do script")
        acao = (f"nao ajustar executores nem codigo Spark por causa da ocupacao: {pct}% do tempo ocioso esta na "
                f"fase {pior['id']} ({pior['duracao_s']} s sem nenhum job), {onde_txt}; se quiser reduzir, "
                "investigar o que roda nesse trecho (subida da sessao, imports, chamadas fora do Spark)")
        confirmar = (f"recalcular a ocupacao so nas fases com jobs: {ocup_com_jobs}%"
                     if ocup_com_jobs is not None else "comparar a ocupacao das fases com e sem jobs")
    else:
        acao = (f"investigar a fase {pior['id']} (linhas {trecho[0] if trecho else '?'}-"
                f"{trecho[1] if trecho else '?'}): {pior['ocupacao_pct']}% de ocupacao com {pior['jobs']} jobs "
                f"concentra {pct}% do tempo ocioso")
        confirmar = f"comparar a ocupacao da fase {pior['id']} ({pior['ocupacao_pct']}%) apos a mudanca"
    return {
        "tipo": "ociosidade_por_fase",
        "titulo": f"{pct}% do tempo ocioso esta na fase {pior['id']} ({pior['duracao_s']} s, {pior['jobs']} jobs)",
        "custo_s": pior["duracao_s"],
        "custo_origem": f"plano.fases[{pior['id']}].duracao_s",
        "mecanismo": f"ocupacao da fase {pior['id']}: {pior.get('ocupacao_pct')}%; ocupacao nas fases com jobs: "
                     f"{ocup_com_jobs}%; ocupacao total do log: "
                     f"{(relatorio.get('capacidade_cluster') or {}).get('ocupacao_pct')}%",
        "onde": {"limites_da_fase": [x for x in trecho if x]} if trecho else {},
        "acao": acao,
        "como_confirmar": confirmar,
        "ligacoes": {"fase_log": "janela_de_tempo", "fase_script": "mensagem_do_script"},
        "confianca": "alta",
        "refs": [pior["id"], achado],
    }


def _candidatas(sinais: list[dict], cruzamentos: list[dict], fases: list[dict], relatorio: dict) -> list[dict]:
    stages = {s["stage_key"]: s for s in relatorio.get("stages", [])}
    sinal = {s["id"]: s for s in sinais}
    cands = []

    for x in cruzamentos:
        s = sinal.get(x["sinal"], {})
        log, scr = x.get("log") or {}, x.get("script") or {}
        if s.get("regra") == "tabela_lida_varias_vezes" and log.get("stages") and scr.get("dataframe_reutilizado") \
                and not scr.get("cache_ou_persist"):
            var, fim = scr["dataframe_reutilizado"], scr.get("linha_fim_definicao") or scr["linha_definicao"]
            usos = [u["linha"] for u in scr.get("usado_por", [])]
            cands.append({
                "tipo": "releitura_sem_cache",
                "titulo": f"{var} e recalculado {s['leituras']}x (le {s['tabela']} a cada uso)",
                "custo_s": log["duracao_stages_s"],
                "custo_origem": f"plano.cruzamentos[{x['id']}].log.duracao_stages_s",
                "mecanismo": f"plano.sinais[{s['id']}]: {s['leituras']} leituras com "
                             f"{', '.join(s.get('operacoes_repetidas', []))}",
                "onde": {"definicao": scr["linha_definicao"], "fim_definicao": fim, "usos": usos},
                "acao": f"persistir {var} ao fim da definicao (linha {fim}) e liberar com unpersist apos a "
                        "ultima acao que o usa",
                "como_confirmar": f"na proxima execucao o log deve ter 1 scan de {log['arquivos_por_scan']} "
                                  f"arquivos em vez de {log['scans']}, e o plano deve trocar as leituras por "
                                  "InMemoryTableScan",
                "ligacoes": {"plano_log": x.get("ligacao_plano_log"), "plano_script": x.get("ligacao_plano_script")},
                "confianca": "alta" if x.get("ligacao_plano_log") == "multiplicidade" else "media",
                "refs": [x["id"], s["id"]],
            })
        elif s.get("regra") == "broadcast_de_dataframe_em_memoria" and log.get("stages") \
                and scr.get("linhas_create_dataframe"):
            pares = [(k, n) for k, n in zip(log["stages"], log.get("tasks_por_stage", [])) if n and n > 1]
            if not pares:
                continue
            custo = round(sum(stages.get(k, {}).get("duration_s") or 0 for k, _ in pares), 3)
            linhas = scr["linhas_create_dataframe"]
            cands.append({
                "tipo": "dataframe_em_memoria_com_muitas_particoes",
                "titulo": f"broadcast de DataFrame criado em memoria executado em {len(pares)} stage(s) de "
                          f"{max(n for _, n in pares)} tasks",
                "custo_s": custo,
                "custo_origem": f"stages {', '.join(k for k, _ in pares)}",
                "mecanismo": f"plano.sinais[{s['id']}]: {s['ocorrencias']} broadcast(s) sobre Scan ExistingRDD",
                "onde": {"create_dataframe": linhas},
                "acao": f"verificar o numero de particoes do DataFrame da linha {linhas[0]} e, se for alto para "
                        "poucas linhas, reduzir com coalesce(1) antes do broadcast",
                "como_confirmar": f"df.rdd.getNumPartitions() no DataFrame da linha {linhas[0]}; apos o ajuste, "
                                  f"os stages de {max(n for _, n in pares)} tasks devem cair para 1 task",
                "ligacoes": {"plano_log": x.get("ligacao_plano_log"), "plano_script": x.get("ligacao_plano_script")},
                "confianca": "media" if x.get("ligacao_plano_log") == "tipo" else "alta",
                "refs": [x["id"], s["id"]],
            })

    for f in fases:
        ops = f.get("operacoes") or []
        manut = [o for o in ops if o["grupo"] == "manutencao_delta"]
        driver = [o for o in ops if o["grupo"] == "acao_no_driver"]
        if not (manut or driver):
            continue
        alvo = manut or driver
        descr = ", ".join(f"{o['metodo']}({o.get('args', '')}) na linha {o['linha']}" for o in alvo)
        maior = f.get("maior_stage")
        extra = (f"; o maior stage da fase e o {maior['stage_key']} ({maior['num_tasks']} tasks, "
                 f"{maior['duration_s']} s)" if maior else "")
        if f.get("achados"):
            extra += f"; achados nesta fase: {', '.join(f['achados'])}"
        if manut:
            acao = (f"avaliar tirar {', '.join(o['metodo'] for o in manut)} do caminho critico do job "
                    "(rotina separada ou menor frequencia)")
        else:
            acao = (f"verificar se {', '.join(o['metodo'] for o in driver)} e necessario ou e resto de "
                    "depuracao; cada acao dispara um job completo")
        cands.append({
            "tipo": "manutencao_delta_no_job" if manut else "acao_no_driver",
            "titulo": f"trecho entre as mensagens '{f['de'][:50]}' e '{f['ate'][:50]}' levou {f['duracao_s']} s",
            "custo_s": f["duracao_s"],
            "custo_origem": f"plano.fases[{f['id']}].duracao_s",
            "mecanismo": f"operacoes no trecho: {descr}{extra}",
            "onde": {"trecho": f["linhas_script"], "operacoes": [o["linha"] for o in alvo]},
            "acao": acao,
            "como_confirmar": f"rodar sem {alvo[0]['metodo']} (ou com ele em rotina separada) e comparar a "
                              f"duracao da fase {f['id']} ({f['duracao_s']} s) e os jobs/stages dela no log",
            "ligacoes": {"fase_log": "janela_de_tempo", "fase_script": "mensagem_do_script"},
            "confianca": "media",
            "refs": [f["id"]] + f.get("achados", []),
        })

    c = _candidata_ociosidade(fases, relatorio)
    if c:
        cands.append(c)

    cands.sort(key=lambda c: -(c.get("custo_s") or 0))
    for i, c in enumerate(cands, start=1):
        c["id"] = f"C{i}"
    return [{"id": c.pop("id"), **c} for c in cands]


# ======================================================================= entrada
def montar_plano(caminho: Path, eventos, relatorio: dict, sanitizador: Sanitizador,
                 script: Path | None = None) -> dict:
    """Le o stdout e devolve o bloco ``plano`` do resumo enviado a IA."""
    caminho = Path(caminho)
    bruto = caminho.read_bytes()
    linhas = [_limpar(l) for l in bruto.decode("utf-8", errors="replace").splitlines()]
    blocos, fora, avisos = _separar(linhas)
    marcos = _marcos(fora, sanitizador)

    planos, sinais = [], []
    for n, bloco in enumerate(blocos, start=1):
        p = _analisar_plano(bloco, None)
        sinais += _sinais(p, n)
        p["indice"] = n
        planos.append(p)

    fonte_script, sc = None, None
    if script:
        fonte_script = Path(script).read_bytes().decode("utf-8", errors="replace")
        try:
            sc = _Script(fonte_script)
        except SyntaxError:
            sc = None
    cruz_log = _cruzar_log(sinais, eventos, relatorio)
    cruz_script = _cruzar_script(sinais, fonte_script, Path(script).name if script else None)

    cruzamentos = []
    por_sinal: dict[str, dict] = defaultdict(dict)
    for c in cruz_log:
        por_sinal[c["sinal"]]["log"] = c
    for c in cruz_script:
        if "sinal" in c:
            por_sinal[c["sinal"]]["script"] = c
        else:
            avisos.append(c.get("motivo", "script nao analisado"))
    for i, (sinal, partes) in enumerate(por_sinal.items(), start=1):
        x = {"id": f"X{i}", "sinal": sinal}
        if "log" in partes:
            l = partes["log"]
            x["ligacao_plano_log"] = l["ligacao_plano_log"]
            x["explicacao_ligacao_log"] = l["explicacao"]
            x["log"] = l["log"]
        if "script" in partes:
            s_ = dict(partes["script"]); s_.pop("sinal", None)
            x["ligacao_plano_script"] = s_.pop("ligacao_plano_script")
            x["script"] = s_
        cruzamentos.append(x)

    for p in planos:
        p.pop("_interno", None)
    if not blocos and not avisos:
        avisos.append("Nenhum '== Physical Plan ==' encontrado no stdout (o script chamou df.explain()?).")
    if any(s.get("tabela_truncada") for s in sinais) or any(
            n.get("tabela_truncada") for p in planos for n in p.get("nos", [])):
        avisos.append("Nomes de tabela truncados no plano ('...'): aumente "
                      "spark.sql.maxMetadataStringLength no job para o nome sair completo.")
    if any(p["plano_final_aqe"] is False for p in planos):
        avisos.append("Plano impresso ANTES da execucao (isFinalPlan=false): o AQE pode ter trocado "
                      "estrategias de join e coalescido particoes durante a execucao.")

    coerencia = _coerencia(marcos, relatorio)
    fases = _fases(marcos, eventos, relatorio, sc, coerencia.get("status") == "ok")
    candidatas = _candidatas(sinais, cruzamentos, fases, relatorio)

    return sanitizador.sanitizar({
        "plano_version": PLANO_VERSION,
        "fonte": {"arquivo": caminho.name, "sha256": hashlib.sha256(bruto).hexdigest(),
                  "planos_encontrados": len(planos), "avisos": avisos,
                  "coerencia_com_stderr": coerencia,
                  "secoes_logicas": "Parsed/Analyzed/Optimized nao sao enviadas; so o plano fisico"},
        "marcos": marcos,
        "planos": planos,
        "sinais": sinais,
        "cruzamentos": cruzamentos,
        "fases": fases,
        "candidatas": candidatas,
    })


def reduzir_plano(plano: dict) -> dict:
    """Versao menor para o limite de tokens: sem a lista de nos (sinais e cruzamentos ficam)."""
    novo = dict(plano)
    novo["planos"] = [{k: v for k, v in p.items() if k != "nos"} | {"nos_omitidos": len(p.get("nos", []))}
                      for p in plano.get("planos", [])]
    novo["reduzido"] = True
    return novo
