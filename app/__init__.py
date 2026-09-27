"""Parser de logs do Spark com deteccao de oportunidades de otimizacao."""
from .parser import LogEvent, parse_log
from .relatorio import imprimir_resumo, montar_relatorio
from .sanitizador import Sanitizador

__all__ = ["LogEvent", "parse_log", "Sanitizador", "montar_relatorio", "imprimir_resumo"]
