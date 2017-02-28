package org.apache.ignite.math.impls;

import org.apache.ignite.math.Matrix;
import org.apache.ignite.math.UnsupportedOperationException;
import org.apache.ignite.math.Vector;
import org.apache.ignite.math.impls.storage.matrix.SparseLocalRowMatrixStorage;

import java.util.Map;

/**
 * TODO: add description
 */
public class SparseLocalOnHeapRowMatrix extends AbstractMatrix{
    /** */
    public SparseLocalOnHeapRowMatrix(Map<String, Object> args) {
        assert args != null;

        boolean accessMode = (boolean) args.getOrDefault("randomAccess", true);

        if (args.containsKey("rows") && args.containsKey("cols"))
            setStorage(new SparseLocalRowMatrixStorage((int)args.get("rows"), (int)args.get("cols"), accessMode));
        else if (args.containsKey("arr"))
            setStorage(new SparseLocalRowMatrixStorage((double[][])args.get("arr"), accessMode));
        else
            throw new UnsupportedOperationException("Invalid constructor argument(s).");
    }

    /** */
    SparseLocalOnHeapRowMatrix(int rows, int cols, boolean randomAccess){
        setStorage(new SparseLocalRowMatrixStorage(rows, cols, randomAccess));
    }

    @Override
    public Matrix copy() {
        Matrix copy = like(rowSize(), columnSize());

        copy.assign(this);

        return copy;
    }

    /** {@inheritDoc} */
    @Override
    public Matrix like(int rows, int cols) {
        return new SparseLocalOnHeapRowMatrix(rows, cols, getStorage().isSequentialAccess());
    }

    /** {@inheritDoc} */
    @Override
    public Vector likeVector(int crd) {
        return isSequentialAccess() ?
                new SequentialAccessSparseLocalOnHeapVector(crd) :
                new RandomAccessSparseLocalOnHeapVector(crd);
    }
}
