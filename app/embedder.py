import asyncio
import logging

from fastembed import TextEmbedding
from fastembed import SparseTextEmbedding
from qdrant_client.http import models as qm

from .config import settings


log = logging.getLogger(__name__)

_model: TextEmbedding | None = None
_sparse_model: SparseTextEmbedding | None = None


def _get_model() -> TextEmbedding:
    global _model
    if _model is None:
        log.info("loading fastembed model %s", settings.embed_model)
        _model = TextEmbedding(model_name=settings.embed_model)
    return _model


def _get_sparse_model() -> SparseTextEmbedding:
    global _sparse_model
    if _sparse_model is None:
        log.info("loading sparse fastembed model %s", settings.sparse_embed_model)
        _sparse_model = SparseTextEmbedding(model_name=settings.sparse_embed_model)
    return _sparse_model


def _embed_sync(texts: list[str]) -> list[list[float]]:
    model = _get_model()
    return [vec.tolist() for vec in model.embed(texts)]


def _embed_sparse_sync(texts: list[str]) -> list[qm.SparseVector]:
    model = _get_sparse_model()
    return [
        qm.SparseVector(indices=vec.indices.tolist(), values=vec.values.tolist())
        for vec in model.embed(texts)
    ]


async def embed(texts: list[str]) -> list[list[float]]:
    if not texts:
        return []
    return await asyncio.to_thread(_embed_sync, texts)


async def embed_sparse(texts: list[str]) -> list[qm.SparseVector]:
    if not texts:
        return []
    return await asyncio.to_thread(_embed_sparse_sync, texts)


async def embed_one(text: str) -> list[float]:
    out = await embed([text])
    return out[0]


async def embed_sparse_one(text: str) -> qm.SparseVector:
    out = await embed_sparse([text])
    return out[0]


async def warmup() -> None:
    await asyncio.to_thread(_get_model)
    await asyncio.to_thread(_get_sparse_model)
