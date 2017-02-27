package org.apache.ignite.math.impls.storage;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.math.MatrixStorage;
import org.apache.ignite.math.Vector;

/**
 * TODO: add description
 */
public class SparseLocalRowMatrixStorage implements MatrixStorage {
    private Vector[] rowVectors;

    @Override public double get(int x, int y) {
        return 0;
    }

    @Override public void set(int x, int y, double v) {

    }

    @Override public int columnSize() {
        return 0;
    }

    @Override public int rowSize() {
        return 0;
    }

    @Override public void writeExternal(ObjectOutput out) throws IOException {

    }

    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

    }

    @Override public boolean isSequentialAccess() {
        return false;
    }

    @Override public boolean isDense() {
        return false;
    }

    @Override public double getLookupCost() {
        return 0;
    }

    @Override public boolean isAddConstantTime() {
        return false;
    }

    @Override public boolean isArrayBased() {
        return false;
    }

    @Override public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override public int hashCode() {
        return super.hashCode();
    }
}
