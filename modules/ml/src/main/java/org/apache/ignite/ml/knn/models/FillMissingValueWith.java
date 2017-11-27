package org.apache.ignite.ml.knn.models;

public enum FillMissingValueWith {
    /**
     * Fill missed value with zero or empty string or default value for categorical features
     */
    ZERO,
    /**
     * Fill missed value with mean on column
     * Requires an additional time to calculate
     */
    MEAN,
    /**
     * Fill missed value with mode on column
     * Requires an additional time to calculate
     */
    MODE,
    /**
     * Deletes observation with missed values
     * Transforms dataset and changes indexing
     */
    DELETE
}
