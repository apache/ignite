package org.apache.ignite.cache.query;
/**
 * Hybrid ranking strategies for combining text and vector scores.
 */
public enum HybridStrategy {
    /**
     * Reciprocal Rank Fusion (RRF).
     * Combines rankings from text and vector searches using reciprocal ranks.
     * Formula: score = 1/(k + rank_text) + 1/(k + rank_vector)
     */
    RRF,

    /**
     * Weighted sum of normalized scores.
     * Formula: score = text_weight * text_score + vector_weight * vector_score
     */
    WEIGHTED_SUM
}