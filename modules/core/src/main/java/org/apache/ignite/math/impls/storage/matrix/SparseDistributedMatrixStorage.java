// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.math.impls.storage.matrix;

import org.apache.ignite.math.*;
import java.io.*;

/**
 * TODO: add description.
 */
public class SparseDistributedMatrixStorage implements MatrixStorage, StorageConstants {
    private int rows, cols;
    private int stoMode, acsMode;

    /**
     *
     */
    public SparseDistributedMatrixStorage() {
        // No-op.
    }

    /**
     *
     * @param rows
     * @param cols
     * @param stoMode
     * @param acsMode
     */
    public SparseDistributedMatrixStorage(int rows, int cols, int stoMode, int acsMode) {
        assert rows > 0;
        assert cols > 0;
        assertAccessMode(acsMode);
        assertStorageMode(stoMode);

        this.rows = rows;
        this.cols = cols;
        this.stoMode = stoMode;
        this.acsMode = acsMode;
    }

    @Override
    public double get(int x, int y) {
        return 0; // TODO
    }

    @Override
    public void set(int x, int y, double v) {
        // TODO
    }

    @Override
    public int columnSize() {
        return cols;
    }

    @Override
    public int rowSize() {
        return rows;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        // TODO
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        // TODO
    }

    @Override
    public boolean isSequentialAccess() {
        return acsMode == SEQUENTIAL_ACCESS_MODE;
    }

    @Override
    public boolean isDense() {
        return false;
    }

    @Override
    public boolean isRandomAccess() {
        return true;
    }

    @Override
    public boolean isDistributed() {
        return true;
    }

    @Override
    public boolean isArrayBased() {
        return false; 
    }
}
