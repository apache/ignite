package org.apache.ignite.math.exceptions;

import org.apache.ignite.IgniteException;

/**
 * TODO: add description.
 */
public class NonSymmetricMatrixException extends IgniteException {
    /**
     * @param row Row.
     * @param column Column.
     * @param threshold Threshold.
     */
    public NonSymmetricMatrixException(int row, int column, double threshold) {
        super("Symmetric matrix expected, the symmetry is broken on row " + row + " and col " + column + " with this threshold " + threshold);
    }
}
