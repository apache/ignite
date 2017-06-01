package org.apache.ignite.ml.math.impls.storage.matrix;

import org.apache.ignite.internal.util.GridArgumentCheck;
import org.apache.ignite.ml.math.VectorStorage;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import java.util.Set;

/**
 * Storage for wrapping given map.
 */
public class MapWrapperStorage implements VectorStorage {
    /** Underlying map. */
    Map<Integer, Double> data;

    /** Vector size. */
    int size;

    /**
     * Construct a wrapper around given map.
     *
     * @param map Map to wrap.
     */
    public MapWrapperStorage(Map<Integer, Double> map) {
        Set<Integer> keys = map.keySet();

        GridArgumentCheck.notEmpty(keys, "map");

        Integer min = keys.stream().mapToInt(Integer::valueOf).min().getAsInt();
        Integer max = keys.stream().mapToInt(Integer::valueOf).max().getAsInt();

        assert min >= 0;

        data = map;
        size = (max - min) + 1;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return size;
    }

    /** {@inheritDoc} */
    @Override public double get(int i) {
        return data.getOrDefault(i, 0.0);
    }

    /** {@inheritDoc} */
    @Override public void set(int i, double v) {
        if (v != 0.0)
            data.put(i, v);
        else if (data.containsKey(i))
            data.remove(i);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(data);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        data = (Map<Integer, Double>) in.readObject();
    }

    /** {@inheritDoc} */
    @Override public boolean isSequentialAccess() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isRandomAccess() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isDense() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isArrayBased() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isDistributed() {
        return false;
    }
}
