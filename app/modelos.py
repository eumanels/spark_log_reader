from __future__ import annotations
from enum import Enum
from typing import Literal, Union
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

Valor = Union[str, int, float, bool, None]


class Classificacao(str, Enum):
    SAUDAVEL = "saudavel"
    DEGRADADA = "degradada"
    COM_FALHA = "com_falha"
    INCONCLUSIVA = "inconclusiva"


class _Base(BaseModel):
    # extra="forbid": campo inventado pela IA invalida a resposta.
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class Evidencia(_Base):
    caminho: str = Field(min_length=1, description="Caminho do valor no resumo_ia.json, ex.: stages[88.0].skew_ratio")
    valor: Valor = Field(description="Valor exatamente como aparece no resumo (sem arredondar).")


class Gargalo(_Base):
    id: str = Field(pattern=r"^G\d+$", description="G1, G2, ...")
    titulo: str = Field(min_length=3, max_length=160)
    evidencias: list[Evidencia] = Field(min_length=1)
    achado_relacionado: str | None = Field(default=None, pattern=r"^A\d+$",
                                           description="id do achado do resumo (A1, A2...), se houver")
    indicacao: str = Field(min_length=10, description="O que a metrica sugere e o que investigar, "
                                                      "em linguagem de hipotese.")
    prioridade: Literal["Alta", "Media", "Baixa"]

    @field_validator("prioridade", mode="before")
    @classmethod
    def _sem_acento(cls, v):
        # A IA costuma escrever "Média"; aceitamos e normalizamos.
        return {"Média": "Media", "média": "Media", "alta": "Alta", "baixa": "Baixa"}.get(v, v)


class Recomendacao(_Base):
    id: str = Field(pattern=r"^R\d+$", description="R1, R2, ...")
    tipo: Literal["investigar", "otimizar"] = Field(
        description="investigar = confirmar uma hipotese; otimizar = mudanca concreta sustentada pela evidencia")
    texto: str = Field(min_length=10)
    sustentada_por: list[str] = Field(min_length=1, description="ids dos gargalos (G1...) que justificam")


class AnaliseIA(_Base):
    classificacao: Classificacao
    justificativa_classificacao: str = Field(min_length=10)
    evidencias_classificacao: list[Evidencia] = Field(min_length=1)
    gargalos: list[Gargalo] = Field(description="Ordenados do mais para o menos prioritario. Pode ser vazio.")
    recomendacoes: list[Recomendacao]
    dados_insuficientes: list[str] = Field(
        description="Analises importantes que o resumo nao permite fazer (ex.: GC, spill).")

    @model_validator(mode="after")
    def _referencias_consistentes(self):
        ids_g = [g.id for g in self.gargalos]
        ids_r = [r.id for r in self.recomendacoes]
        if len(set(ids_g)) != len(ids_g):
            raise ValueError("ids de gargalos repetidos")
        if len(set(ids_r)) != len(ids_r):
            raise ValueError("ids de recomendacoes repetidos")
        for r in self.recomendacoes:
            faltando = [g for g in r.sustentada_por if g not in ids_g]
            if faltando:
                raise ValueError(f"recomendacao {r.id} cita gargalos inexistentes: {faltando}")
        return self


def schema_json() -> dict:
    """JSON Schema do contrato: vai no prompt e pode ir para o README."""
    return AnaliseIA.model_json_schema()