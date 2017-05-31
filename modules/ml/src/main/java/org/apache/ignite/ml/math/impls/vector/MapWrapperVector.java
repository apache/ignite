package org.apache.ignite.ml.math.impls.vector;

import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.impls.storage.matrix.MapWrapperStorage;

import java.util.Map;

/**
 * Vector wrapping a given map.
 */
public class MapWrapperVector extends AbstractVector {

    /**
     * Construct a vector wrapping given map.
     *
     * @param map Map to wrap.
     */
    public MapWrapperVector(Map<Integer, Double> map) {
        setStorage(new MapWrapperStorage(map));
    }

    /** {@inheritDoc} */
    @Override public Vector like(int crd) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Matrix likeMatrix(int rows, int cols) {
        throw new UnsupportedOperationException();
    }
}
