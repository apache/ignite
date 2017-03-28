package org.apache.ignite.math.exceptions;

import org.apache.ignite.IgniteException;

/**
 * TODO: add description.
 */
public class NonPositiveDefiniteMatrixException extends IgniteException {
    /**
     * Construct an exception.
     *
     * @param wrong Value that fails the positivity check.
     * @param index Row (and column) index.
     * @param threshold Absolute positivity threshold.
     */
    public NonPositiveDefiniteMatrixException(double wrong, int index, double threshold) {
        super("Matrix must be positive, wrong element located on diagonal with index " + index+ " and have value " + wrong + " with this threshold " + threshold);
    }
}
