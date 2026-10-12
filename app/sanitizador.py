from __future__ import annotations

import ipaddress
import re

from .padroes import (
    ARN_PATTERN,
    HOST_EC2_PATTERN,
    IPV4_PATTERN,
    IPV6_COLCHETES_PATTERN,
    IPV6_SOLTO_PATTERN,
    PATH_PATTERN,
)


def _ip_normalizado(texto: str) -> str | None:
    try:
        ip = ipaddress.ip_address(texto)
    except ValueError:
        return None
    if ip.is_unspecified or ip.is_loopback:
        return None
    return str(ip)


class Sanitizador:
    def __init__(self):
        self.host_map: dict[str, str] = {}
        self.path_map: dict[str, str] = {}

    # ------------------------------------------------------------------ hosts
    def _pseudonimo_host(self, chave: str) -> str:
        if chave not in self.host_map:
            self.host_map[chave] = f"host_{len(self.host_map) + 1}"
        return self.host_map[chave]

    def mask_host(self, host: str) -> str:
        """Mascara um campo que SEMPRE e host (ex.: host de task). Remove colchetes e porta."""
        h = host.strip()
        m = re.fullmatch(r"\[(.+)\](?::\d+)?", h)
        if m:
            h = m.group(1)
        elif _ip_normalizado(h) is None and re.search(r":\d+$", h):
            h = h.rsplit(":", 1)[0]
        ec2 = HOST_EC2_PATTERN.fullmatch(h)
        if ec2:
            h = ec2.group(1).replace("-", ".")
        chave = _ip_normalizado(h) or h.lower()
        return self._pseudonimo_host(chave)

    # ------------------------------------------------------------------ paths
    def _pseudonimo_path(self, raw: str) -> str:
        if raw not in self.path_map:
            self.path_map[raw] = f"path_{len(self.path_map) + 1}"
        return self.path_map[raw]

    def mask_paths(self, text: str) -> str:
        text = ARN_PATTERN.sub(lambda m: self._pseudonimo_path(m.group(0)), text)
        return PATH_PATTERN.sub(lambda m: self._pseudonimo_path(m.group(0)), text)

    # ------------------------------------------------------------------ texto
    def mask_text(self, text: str) -> str:
        """Mascara texto livre: paths/ARNs, hostnames EC2 e IPs confirmados."""
        text = self.mask_paths(text)

        def _ec2(m):
            ip = _ip_normalizado(m.group(1).replace("-", "."))
            return f"[{self._pseudonimo_host(ip)}]" if ip else m.group(0)

        def _ip(m):
            ip = _ip_normalizado(m.group(1))
            return f"[{self._pseudonimo_host(ip)}]" if ip else m.group(0)

        text = HOST_EC2_PATTERN.sub(_ec2, text)
        text = IPV6_COLCHETES_PATTERN.sub(_ip, text)
        text = IPV4_PATTERN.sub(_ip, text)
        return IPV6_SOLTO_PATTERN.sub(_ip, text)

    def sanitizar(self, obj):
        """Percorre o relatorio inteiro (dict/list/str) e mascara todo texto.

        As chaves dos dicionarios sao nomes de campo do proprio codigo e nao
        sao alteradas. Use antes de gravar o JSON ou enviar a IA.
        """
        if isinstance(obj, str):
            return self.mask_text(obj)
        if isinstance(obj, list):
            return [self.sanitizar(v) for v in obj]
        if isinstance(obj, tuple):
            return tuple(self.sanitizar(v) for v in obj)
        if isinstance(obj, dict):
            return {k: self.sanitizar(v) for k, v in obj.items()}
        return obj