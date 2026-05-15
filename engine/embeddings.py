"""
Lightweight embedding layer using sentence-transformers all-MiniLM-L6-v2.

Falls back to TF-IDF-style bag-of-words if the model is unavailable,
so the engine works even in restricted environments.
"""
from __future__ import annotations

import hashlib
import re
from functools import lru_cache
from typing import List, Optional

import numpy as np

_MODEL = None
_AVAILABLE = False


def _try_load_model() -> bool:
    global _MODEL, _AVAILABLE
    if _AVAILABLE:
        return True
    try:
        from sentence_transformers import SentenceTransformer
        _MODEL = SentenceTransformer("all-MiniLM-L6-v2")
        _AVAILABLE = True
        return True
    except Exception:
        return False


def embed(texts: List[str]) -> np.ndarray:
    """
    Embed a list of strings. Returns (N, D) float32 array.
    Falls back to BOW fingerprint if model unavailable.
    """
    if not texts:
        return np.zeros((0, 384), dtype=np.float32)

    if _try_load_model():
        try:
            vecs = _MODEL.encode(texts, normalize_embeddings=True, show_progress_bar=False)
            return vecs.astype(np.float32)
        except Exception:
            pass

    # Fallback: character n-gram hash trick (384-dim)
    return _bow_embed(texts)


def cosine_similarity_matrix(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    """Returns (len(a), len(b)) cosine similarity matrix."""
    if a.ndim == 1:
        a = a.reshape(1, -1)
    if b.ndim == 1:
        b = b.reshape(1, -1)
    norms_a = np.linalg.norm(a, axis=1, keepdims=True)
    norms_b = np.linalg.norm(b, axis=1, keepdims=True)
    a_norm = a / (norms_a + 1e-9)
    b_norm = b / (norms_b + 1e-9)
    return a_norm @ b_norm.T


def embed_single(text: str) -> np.ndarray:
    return embed([text])[0]


# ── Fallback BOW ───────────────────────────────────────────────────────────

_DIM = 384
_NGRAM_N = 3


def _bow_embed(texts: List[str]) -> np.ndarray:
    vecs = np.zeros((len(texts), _DIM), dtype=np.float32)
    for i, text in enumerate(texts):
        vecs[i] = _text_to_vec(text)
    return vecs


def _text_to_vec(text: str) -> np.ndarray:
    text = text.lower()
    tokens = re.findall(r"[a-z0-9]+", text)
    vec = np.zeros(_DIM, dtype=np.float32)
    for token in tokens:
        ngrams = [token[j: j + _NGRAM_N] for j in range(len(token) - _NGRAM_N + 1)]
        for ng in ngrams or [token]:
            h = int(hashlib.md5(ng.encode()).hexdigest(), 16)
            idx = h % _DIM
            vec[idx] += 1.0
    norm = np.linalg.norm(vec)
    if norm > 0:
        vec /= norm
    return vec
