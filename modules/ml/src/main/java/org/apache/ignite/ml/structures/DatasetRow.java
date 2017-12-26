package org.apache.ignite.ml.structures;

import org.apache.ignite.ml.math.Vector;

public class DatasetRow<V extends Vector> {
    /** Vector. */
    protected final V vector;

    /** */
    public DatasetRow(V vector) {
        this.vector = vector;
    }

    /**
     * Get the vector.
     *
     * @return Vector.
     */
    public V features() {
        return vector;
    }
}
