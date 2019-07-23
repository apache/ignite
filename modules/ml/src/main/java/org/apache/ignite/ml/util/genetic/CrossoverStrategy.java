package org.apache.ignite.ml.util.genetic;

/**
 * Represents the crossover strategy depending of locus point amount.
 */
public enum CrossoverStrategy {
    /** One point. */
    ONE_POINT,

    /** Two point. */
    TWO_POINT,

    /** Multi point. */
    MULTI_POINT,

    /** Uniform. */
    UNIFORM
}