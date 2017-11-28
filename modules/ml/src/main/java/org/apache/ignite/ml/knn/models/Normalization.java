package org.apache.ignite.ml.knn.models;


public enum Normalization {
    /** Minimax.
     *
     * x'=(x-MIN[X])/(MAX[X]-MIN[X])
     */
    MINIMAX,
    /** Z normalization.
     *
     * x'=(x-M[X])/\sigma [X]
     */
    Z_NORMALIZATION
}
