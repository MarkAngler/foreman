"""Surprisal-based document scoring.

Port of honcho upstream `src/dreamer/surprisal.py`. Honcho's algorithm:

    1. Sample N observations for an (observer, observed) pair (recent, random,
       or all).
    2. Build a tree over their embeddings (default: sklearn KDTree).
    3. For each embedding, compute surprisal as a k-NN density estimate:
           S(e) = dim * log(mean_distance_to_k_nearest + epsilon)
    4. Min-max normalise scores to [0, 1].
    5. Take the top P% (default 20%).

Foreman differences (documented in worktree_notes.md):
  - Honcho stores `embedding` directly on the `documents` table; we don't
    (it's served from the documents Vector Search index). For now we accept
    a callable `embed_fn` so callers can plug in either FMAPI embeddings (from
    `foreman.lib.vector_search.embed`) in production or a pure-Python stub
    in tests. This keeps the algorithm faithful while sidestepping the
    embedding-source bootstrap.
  - We default the tree to `kdtree`, matching honcho's `SklearnTreeWrapper`
    with `tree_type='kd', k=5`. Other tree types (rptree, balltree, lsh,
    covertree, graph, prototype) are honcho-specific and not ported here —
    Phase 3 can add them if profiling shows surprisal quality regressions.
  - We use stdlib + numpy + scikit-learn for the math. If sklearn is
    unavailable at runtime (e.g. a stripped Spark image) we fall back to a
    pure-Python brute-force k-NN that yields the same surprisal values
    within float precision.

Source:
  https://github.com/plastic-labs/honcho/blob/main/src/dreamer/surprisal.py
"""

from __future__ import annotations

import logging
import math
from collections.abc import Callable, Iterable
from dataclasses import dataclass

logger = logging.getLogger(__name__)


# Honcho's defaults from `settings.DREAM.SURPRISAL.*`.
DEFAULT_TREE_K = 5
DEFAULT_SAMPLE_SIZE = 200
DEFAULT_TOP_PERCENT = 0.20
EPSILON = 1e-10
INCLUDE_LEVELS = ("explicit", "deductive")  # honcho's default inclusion set


@dataclass(frozen=True)
class ObservationData:
    """Plain data extracted from a Lakebase document row (mirrors honcho)."""

    id: str
    content: str
    level: str
    observer: str
    observed: str
    workspace_name: str


@dataclass
class SurprisalScore:
    """Container for an observation with its computed surprisal score."""

    observation: ObservationData
    surprisal: float
    embedding: list[float]


# ---- Public entrypoint -------------------------------------------------------


def score_observations(
    observations: list[ObservationData],
    *,
    embed_fn: Callable[[str], list[float]],
    tree_k: int = DEFAULT_TREE_K,
    top_percent: float = DEFAULT_TOP_PERCENT,
) -> list[SurprisalScore]:
    """Compute surprisal scores and return the top-percentage by score.

    Args:
        observations: Documents to score.
        embed_fn: A function that returns an embedding for a content string.
            Production callers pass `foreman.lib.vector_search.embed` (sync
            wrapper); tests pass a deterministic stub.
        tree_k: k for the k-NN density estimate.
        top_percent: Fraction of observations to keep (e.g. 0.2 = top 20%).

    Returns the top scores sorted by surprisal descending. Returns an empty
    list if there are too few observations to build a tree (< 2*k), matching
    honcho's behaviour.
    """
    if not observations:
        return []

    min_observations = tree_k * 2
    if len(observations) < min_observations:
        logger.info(
            "surprisal: too few observations (%d < %d), skipping",
            len(observations),
            min_observations,
        )
        return []

    embeddings = [embed_fn(obs.content) for obs in observations]

    raw_scores = _compute_raw_surprisal(embeddings, k=tree_k)

    valid: list[SurprisalScore] = []
    for obs, emb, raw in zip(observations, embeddings, raw_scores, strict=True):
        if math.isinf(raw) or math.isnan(raw):
            continue
        valid.append(SurprisalScore(observation=obs, surprisal=raw, embedding=emb))

    if len(valid) < len(raw_scores):
        logger.warning(
            "surprisal: filtered %d invalid scores", len(raw_scores) - len(valid)
        )

    normalised = _normalise(valid)
    normalised.sort(key=lambda s: s.surprisal, reverse=True)

    take = max(1, int(len(normalised) * top_percent))
    selected = normalised[:take]
    logger.info(
        "surprisal: selected %d/%d (top %.0f%%)",
        len(selected),
        len(observations),
        top_percent * 100,
    )
    return selected


# ---- k-NN surprisal core (honcho's SklearnTreeWrapper logic, ported) --------


def _compute_raw_surprisal(embeddings: list[list[float]], *, k: int) -> list[float]:
    """Per honcho:  S(e) = dim * log(avg_distance_to_k_nearest + epsilon)."""
    if not embeddings:
        return []
    dim = len(embeddings[0])
    k_actual = min(k, len(embeddings))
    distances = _knn_distances(embeddings, k=k_actual)
    return [dim * math.log(avg + EPSILON) for avg in distances]


def _knn_distances(embeddings: list[list[float]], *, k: int) -> list[float]:
    """Average distance to the k nearest neighbours (including self at 0).

    Tries sklearn KDTree first (matches honcho's tree_type='kd'); falls back
    to a brute-force pure-Python implementation when sklearn isn't installed.
    Both return the same value within float precision.
    """
    try:
        return _knn_distances_sklearn(embeddings, k=k)
    except ImportError:
        logger.warning(
            "surprisal: sklearn unavailable, falling back to brute-force k-NN"
        )
        return _knn_distances_bruteforce(embeddings, k=k)


def _knn_distances_sklearn(embeddings: list[list[float]], *, k: int) -> list[float]:
    import numpy as np
    from sklearn.neighbors import KDTree  # type: ignore[import-untyped]

    arr = np.array(embeddings, dtype=np.float32)
    tree = KDTree(arr)
    distances, _idx = tree.query(arr, k=k)
    return [float(np.mean(row)) for row in distances]


def _knn_distances_bruteforce(embeddings: list[list[float]], *, k: int) -> list[float]:
    out: list[float] = []
    n = len(embeddings)
    for i in range(n):
        dists = sorted(
            _euclidean(embeddings[i], embeddings[j]) for j in range(n)
        )
        # Honcho's tree.query(..., k=k) includes the point itself at distance 0
        # because the tree is built over the same set we're querying.
        out.append(sum(dists[:k]) / k)
    return out


def _euclidean(a: Iterable[float], b: Iterable[float]) -> float:
    return math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b, strict=True)))


# ---- Normalisation -----------------------------------------------------------


def _normalise(scores: list[SurprisalScore]) -> list[SurprisalScore]:
    """Min-max normalise raw surprisal values into [0, 1]."""
    if not scores:
        return []
    values = [s.surprisal for s in scores]
    lo = min(values)
    hi = max(values)
    if hi == lo:
        return [
            SurprisalScore(observation=s.observation, surprisal=0.5, embedding=s.embedding)
            for s in scores
        ]
    span = hi - lo
    return [
        SurprisalScore(
            observation=s.observation,
            surprisal=(s.surprisal - lo) / span,
            embedding=s.embedding,
        )
        for s in scores
    ]
