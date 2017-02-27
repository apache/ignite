package org.apache.ignite.math.impls.storage;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import org.apache.ignite.math.MatrixStorage;
import org.apache.ignite.math.Vector;
import org.apache.ignite.math.impls.RandomAccessSparseLocalOnHeapVector;
import org.apache.ignite.math.impls.SequentialAccessSparseLocalOnHeapVector;

/**
 * TODO: add description
 */
public class SparseLocalRowMatrixStorage implements MatrixStorage {
    private Vector[] rowVectors;

    private int rows;
    private int columns;
    private boolean randomAccessRows;

    /** For serialization. */
    public SparseLocalRowMatrixStorage(){
        // No-op.
    }

    /**
     * Create new empty sparse matrix storage.
     *
     * @param rows Rows.
     * @param cols Columns.
     * @param randomAccess Random access.
     */
    public SparseLocalRowMatrixStorage(int rows, int cols, boolean randomAccess){
        this(rows, cols, randomAccess ? new RandomAccessSparseLocalOnHeapVector[rows] : new SequentialAccessSparseLocalOnHeapVector[rows]);
    }

    /**
     * * Create new sparse matrix storage from provide row vectors.
     *
     * @param rows Rows.
     * @param cols Columns.
     * @param vectors Vectors.
     */
    public SparseLocalRowMatrixStorage(int rows, int cols, Vector[] vectors){
        this(rows, cols, vectors, vectors instanceof RandomAccessSparseLocalOnHeapVector[]);
    }

    /**
     * Create new sparse matrix storage from provide row vectors.
     *
     * @param rows Rows.
     * @param columns Columns.
     * @param vectors Vectors.
     * @param randomAccess Random access.
     */
    public SparseLocalRowMatrixStorage(int rows, int columns, Vector[] vectors, boolean randomAccess){
        this.rows = rows;
        this.columns = columns;
        this.randomAccessRows = randomAccess;
        this.rowVectors = vectors.clone();

        for (int i = 0; i < rows; i++)

            if (vectors[i] == null)
                vectors[i] = randomAccess
                    ? new RandomAccessSparseLocalOnHeapVector(columns)
                    : new SequentialAccessSparseLocalOnHeapVector(columns);

            else
                this.rowVectors[i] = vectors[i];
    }

    /** {@inheritDoc} */
    @Override public double get(int x, int y) {
        return rowVectors[x].get(y);
    }

    /** {@inheritDoc} */
    @Override public void set(int x, int y, double v) {
        rowVectors[x].set(y, v);
    }

    /** {@inheritDoc} */
    @Override public int columnSize() {
        return columns;
    }

    /** {@inheritDoc} */
    @Override public int rowSize() {
        return rows;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(rows);
        out.writeInt(columns);
        out.writeBoolean(randomAccessRows);
        out.writeObject(rowVectors);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.rows = in.readInt();
        this.columns = in.readInt();
        this.randomAccessRows = in.readBoolean();
        this.rowVectors = (Vector[])in.readObject();
    }

    /** {@inheritDoc} */
    @Override public boolean isSequentialAccess() {
        return !randomAccessRows;
    }

    /** {@inheritDoc} */
    @Override public boolean isDense() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public double getLookupCost() {
        return rowVectors[rows - 1].getLookupCost();
    }

    /** {@inheritDoc} */
    @Override public boolean isAddConstantTime() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isArrayBased() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        return obj!= null && getClass() == obj.getClass() && storageEquals((SparseLocalRowMatrixStorage) obj);
    }

    private boolean storageEquals(SparseLocalRowMatrixStorage obj) {
        return this.columns == obj.columns && this.rows == obj.rows && this.randomAccessRows == obj.randomAccessRows &&
            Arrays.equals(this.rowVectors, obj.rowVectors);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = 1;

        result = result * 37 + rows;
        result = result * 37 + columns;
        result = result * 37 + Arrays.hashCode(rowVectors);
        result = result * 37 + Boolean.hashCode(randomAccessRows);

        return result;
    }
}
