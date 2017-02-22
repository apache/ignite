package org.apache.ignite.math.impls.storage;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.math.MatrixStorage;
import org.apache.ignite.math.Vector;
import org.apache.ignite.math.impls.RandomAccessSparseLocalOnHeapVector;

/**
 * Storage for sparse local matrix. Based on RandomAccessSparseVector as rows.
 */
public class SparseLocalMatrixStorage implements MatrixStorage {
    private Int2ObjectOpenHashMap<Vector> rowVectors;

    private int rows;

    private int cols;

    /** For serialization. */
    public SparseLocalMatrixStorage(){
        // No-op.
    }

    /**
     * Create empty matrix with given dimensions.
     *
     * @param rows Rows.
     * @param cols Columns.
     */
    public SparseLocalMatrixStorage(int rows, int cols) {
        this.rows = rows;
        this.cols = cols;
        initDataStorage();
    }

    /**
     * Create new Sparse local matrix from given 2d array.
     *
     * @param data Data.
     */
    public SparseLocalMatrixStorage(double[][] data) {
        this(data.length, data[0].length);

        for (int i = 0; i < rows; i++)
            for (int j = 0; j < cols; j++)
                set(i, j, data[i][j]);
    }

    /** {@inheritDoc} */
    @Override public double get(int x, int y) {
        return rowVectors.get(x).get(y);
    }

    /** {@inheritDoc} */
    @Override public void set(int x, int y, double v) {
        Vector row = rowVectors.get(x);
        row.set(y, v);
    }

    /** {@inheritDoc} */
    @Override public int columnSize() {
        return cols;
    }

    /** {@inheritDoc} */
    @Override public int rowSize() {
        return rows;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(rows);
        out.writeInt(cols);

        for (int i = 0; i < rows; i++)
            out.writeObject(rowVectors.get(i));
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        rows = in.readInt();
        cols = in.readInt();

        rowVectors = new Int2ObjectOpenHashMap<>();

        for (int i = 0; i < cols; i++)
            rowVectors.put(i, (Vector)in.readObject());
    }

    /** {@inheritDoc} */
    @Override public boolean isSequentialAccess() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isDense() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public double getLookupCost() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public boolean isAddConstantTime() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isArrayBased() {
        return false;
    }

    @Override public boolean equals(Object obj) {
        return obj!=null && getClass() == obj.getClass() &&
            (rows == ((SparseLocalMatrixStorage)obj).rows) &&
            (cols == ((SparseLocalMatrixStorage)obj).cols) &&
            (rows == 0 || cols == 0 || ((SparseLocalMatrixStorage)obj).rowVectors.equals(rowVectors));
    }

    @Override public int hashCode() {
        int result = 1;
        result = result * 37 + cols;
        result = result * 37 + rows;
        result = result * 37 + rowVectors.hashCode();
        return result;
    }

    /**
     * Init all row objects.
     */
    private void initDataStorage(){
        rowVectors = new Int2ObjectOpenHashMap();

        for (int i = 0; i < rows; i++)
            rowVectors.put(i, new RandomAccessSparseLocalOnHeapVector(cols));
    }
}
