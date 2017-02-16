package org.apache.ignite.math.impls.storage;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.math.VectorStorage;

/**
 * TODO wip
 */
public class SequentialAccessSparseVectorStorage implements VectorStorage {
    /** {@inheritDoc} */
    @Override public int size() {
        return 0;// TODO wip
    }

    /** {@inheritDoc} */
    @Override public double get(int i) {
        return 0;// TODO wip
    }

    /** {@inheritDoc} */
    @Override public void set(int i, double v) {
        // TODO wip
    }

    /** {@inheritDoc} */
    @Override public double[] data() {
        return new double[0];// TODO wip
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        // TODO wip
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        // TODO wip
    }

    /** {@inheritDoc} */
    @Override public boolean isSequentialAccess() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isDense() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public double getLookupCost() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public boolean isAddConstantTime() {
        return false; // TODO wip
    }

    /** {@inheritDoc} */
    @Override public boolean isArrayBased() {
        return false;
    }
}
