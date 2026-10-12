from __future__ import annotations
import hashlib
import io
import platform
from datetime import datetime, timezone
from pathlib import Path
from . import padroes, validador

PARSER_VERSION = "0.4.0"  
SCHEMA_VERSION = "2.0"     


class _LeitorComHash(io.RawIOBase):

    def __init__(self, bruto):
        self._bruto = bruto
        self.sha256 = hashlib.sha256()
        self.total_bytes = 0

    def readable(self) -> bool:
        return True

    def readinto(self, buffer) -> int:
        n = self._bruto.readinto(buffer)
        if n:
            self.sha256.update(memoryview(buffer)[:n])
            self.total_bytes += n
        return n or 0

    def close(self) -> None:
        self._bruto.close()
        super().close()


def abrir_log(caminho: Path) -> tuple[io.TextIOWrapper, _LeitorComHash]:
    """Abre o log em modo texto (UTF-8, errors='replace') calculando o hash.

    Uso:
        texto, leitor = abrir_log(caminho)
        with texto:
            eventos = parse_log(texto, ...)
        leitor.sha256.hexdigest()   # so depois de ler tudo
    """
    leitor = _LeitorComHash(open(caminho, "rb"))
    texto = io.TextIOWrapper(io.BufferedReader(leitor), encoding="utf-8", errors="replace")
    return texto, leitor


def limiares_vigentes() -> dict:
    """Todos os numeros que influenciam o resultado, num lugar so."""
    return {
        "deteccao": dict(padroes.LIMIARES),
        "validacao": {
            "fracao_minima_cabecalho": validador.FRACAO_MINIMA_CABECALHO,
            "tolerancia_duracao_s": validador.TOLERANCIA_DURACAO_S,
        },
        "score": dict(padroes.PESOS_SCORE),
        "leitura": {
            "limite_stack_linhas": padroes.LIMITE_STACK_LINHAS,
            "limite_continuacao_chars": padroes.LIMITE_CONTINUACAO_CHARS,
            "limite_rdd_desc_chars": padroes.LIMITE_RDD_DESC_CHARS,
        },
    }


def montar_meta(caminho_log: Path, leitor: _LeitorComHash) -> dict:
    return {
        "schema_version": SCHEMA_VERSION,
        "parser_version": PARSER_VERSION,
        "log_nome": Path(caminho_log).name,
        "log_tamanho_bytes": leitor.total_bytes,
        "log_sha256": leitor.sha256.hexdigest(),
        "gerado_em": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
        "python_version": platform.python_version(),
        "limiares": limiares_vigentes(),
    }