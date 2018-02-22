package org.apache.ignite.ml.knn.models;

/**
 * On Heap partition data that keeps part of a linear system.
 */
public class KNNPartitionDataOnHeap implements AutoCloseable {
    /** Part of X matrix. */
    private final double[][] x;

    /** Number of rows. */
    private final int rows;

    /** Number of columns. */
    private final int cols;

    /** Part of Y vector. */
    private final double[] y;

    /**
     * Constructs a new instance of linear system partition data.
     *
     * @param x Part of X matrix.
     * @param rows Number of rows.
     * @param cols Number of columns.
     * @param y Part of Y vector.
     */
    public KNNPartitionDataOnHeap(double[][] x, int rows, int cols, double[] y) {
        this.x = x;
        this.rows = rows;
        this.cols = cols;
        this.y = y;
    }

    /** */
    public double[][] getX() {
        return x;
    }

    /** */
    public int getRows() {
        return rows;
    }

    /** */
    public int getCols() {
        return cols;
    }

    /** */
    public double[] getY() {
        return y;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // Do nothing, GC will clean up.
    }
}
