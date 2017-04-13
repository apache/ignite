package org.apache.ignite.math.exceptions;

import org.apache.ignite.IgniteException;

/**
 * This exception is used to indicate error condition of matrix elements failing the positivity check.
 */
public class NonPositiveDefiniteMatrixException extends IgniteException {
    /**
     * Construct an exception.
     *
     * @param wrong Value that fails the positivity check.
     * @param idx Row (and column) index.
     * @param threshold Absolute positivity threshold.
     */
    public NonPositiveDefiniteMatrixException(double wrong, int idx, double threshold) {
        super("Matrix must be positive, wrong element located on diagonal with index "
            + idx + " and has value " + wrong + " with this threshold " + threshold);
    }
}
