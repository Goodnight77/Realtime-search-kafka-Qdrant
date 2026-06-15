import logging

from qdrant_client import AsyncQdrantClient
from qdrant_client.http import models as qm
from qdrant_client.http.exceptions import UnexpectedResponse

from .config import settings


log = logging.getLogger(__name__)

_client: AsyncQdrantClient | None = None
DENSE_VECTOR_NAME = "dense"
SPARSE_VECTOR_NAME = "sparse"


def get_client() -> AsyncQdrantClient:
    global _client
    if _client is None:
        _client = AsyncQdrantClient(
            url=settings.qdrant_url,
            api_key=settings.qdrant_api_key,
            prefer_grpc=False,
        )
    return _client


def _existing_vector_size(info) -> int | None:
    """Read vector size from a Qdrant collection info, single-unnamed vector."""
    vectors = info.config.params.vectors
    if isinstance(vectors, qm.VectorParams):
        return vectors.size
    if isinstance(vectors, dict):
        first = next(iter(vectors.values()), None)
        if isinstance(first, qm.VectorParams):
            return first.size
    return None


def _has_hybrid_schema(info) -> bool:
    vectors = info.config.params.vectors
    sparse_vectors = info.config.params.sparse_vectors or {}
    if not isinstance(vectors, dict):
        return False
    dense = vectors.get(DENSE_VECTOR_NAME)
    return (
        isinstance(dense, qm.VectorParams)
        and dense.size == settings.embed_dim
        and SPARSE_VECTOR_NAME in sparse_vectors
    )


async def ensure_collection() -> None:
    client = get_client()
    existing = await client.get_collections()
    names = {c.name for c in existing.collections}

    if settings.collection_name in names:
        info = await client.get_collection(settings.collection_name)
        size = _existing_vector_size(info)
        if not _has_hybrid_schema(info):
            log.warning(
                "collection %s is not hybrid dense+sparse schema (size=%s want=%d) recreating",
                settings.collection_name,
                size,
                settings.embed_dim,
            )
            await client.delete_collection(settings.collection_name)
            names.discard(settings.collection_name)

    if settings.collection_name not in names:
        await client.create_collection(
            collection_name=settings.collection_name,
            vectors_config={
                DENSE_VECTOR_NAME: qm.VectorParams(
                    size=settings.embed_dim,
                    distance=qm.Distance.COSINE,
                )
            },
            sparse_vectors_config={
                SPARSE_VECTOR_NAME: qm.SparseVectorParams(
                    modifier=qm.Modifier.IDF,
                )
            },
        )
        log.info(
            "created hybrid collection %s (dense dim=%d, sparse=%s)",
            settings.collection_name,
            settings.embed_dim,
            SPARSE_VECTOR_NAME,
        )

    for field_name, field_schema in (
        ("ts", qm.PayloadSchemaType.INTEGER),
        ("source", qm.PayloadSchemaType.KEYWORD),
    ):
        try:
            await client.create_payload_index(
                collection_name=settings.collection_name,
                field_name=field_name,
                field_schema=field_schema,
            )
        except UnexpectedResponse:
            pass


async def close_client() -> None:
    global _client
    if _client is not None:
        await _client.close()
        _client = None
