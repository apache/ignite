package org.apache.ignite.ml.util.genetic;

/**
 * Please, have a look at https://en.wikipedia.org/wiki/Selection_(genetic_algorithm).
 */
public enum SelectionStrategy {
    /** Roulette wheel. */
    ROULETTE_WHEEL,

    /** Truncation. */
    TRUNCATION,

    /** Tournament. */
    TOURNAMENT
}