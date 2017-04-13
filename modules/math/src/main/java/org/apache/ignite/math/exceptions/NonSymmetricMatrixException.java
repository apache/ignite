package org.apache.ignite.math.exceptions;

import org.apache.ignite.IgniteException;

/**
 * This exception is used to indicate error condition of matrix failing the symmetry check.
 */
public class NonSymmetricMatrixException extends IgniteException {
    /**
     * @param row Row.
     * @param col Column.
     * @param threshold Threshold.
     */
    public NonSymmetricMatrixException(int row, int col, double threshold) {
        super("Symmetric matrix expected, the symmetry is broken on row "
            + row + " and col " + col + " with this threshold " + threshold);
    }
}
