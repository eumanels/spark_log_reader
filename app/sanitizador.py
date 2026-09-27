"""Mascaramento de hosts/IPs e paths s3a:// antes de gravar o resultado."""
from __future__ import annotations

import re

from .padroes import HOST_PATTERN, PATH_PATTERN


class Sanitizador:
    def __init__(self):
        self.host_map: dict[str, str] = {}
        self.path_map: dict[str, str] = {}

    def mask_host(self, host: str) -> str:
        m = re.match(r"\[?([0-9a-fA-F:\.]+?)\]?(?::\d+)?$", host)
        normalized = m.group(1) if m else host
        if normalized not in self.host_map:
            self.host_map[normalized] = f"host_{len(self.host_map) + 1}"
        return self.host_map[normalized]

    def mask_paths(self, text: str) -> str:
        def _replace(m):
            raw = m.group(0)
            if raw not in self.path_map:
                self.path_map[raw] = f"path_{len(self.path_map) + 1}"
            return self.path_map[raw]
        return PATH_PATTERN.sub(_replace, text)

    def mask_text(self, text: str) -> str:
        text = self.mask_paths(text)

        def _replace_host(m):
            return f"[{self.mask_host(m.group(1))}]"
        return HOST_PATTERN.sub(_replace_host, text)
