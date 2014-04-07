/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.indexing.h2.opt;

import org.h2.result.*;
import org.h2.value.*;

/**
 * Row with locking support needed for unique key conflicts resolution.
 */
public class GridH2Row extends Row implements GridSearchRowPointer {
    /** */
    protected static final byte STATE_NEW = 0;

    /** */
    protected static final byte STATE_INSERTED = 1;

    /** */
    protected static final byte STATE_REJECTED = 2;

    /** */
    private volatile byte state;

    /**
     * @param data Column values.
     */
    public GridH2Row(Value... data) {
        this(STATE_NEW, data);
    }

    /**
     * @param state State.
     * @param data Column values.
     */
    public GridH2Row(byte state, Value... data) {
        super(data, MEMORY_CALCULATE);

        this.state = state;
    }

    /**
     * @return True if row was inserted successfully. If row insertion is still in progress then block until it will
     * succeed or fail.
     */
    public final boolean waitInsertComplete() {
        byte s = state;

        if (s == STATE_NEW) {
            synchronized (this) {
                while((s = state) == STATE_NEW) {
                    s = refreshState();

                    if (s != STATE_NEW) {
                        state = s;

                        break;
                    }

                    try {
                        wait(10);
                    }
                    catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();

                        return true;
                    }
                }
            }
        }

        return s == STATE_INSERTED;
    }

    /**
     * Refreshes state.
     *
     * @return Refreshed state.
     */
    protected byte refreshState() {
        return STATE_NEW;
    }

    /**
     * Sets whether insert failed due to unique index violation.
     *
     * @param success Flag value.
     */
    public synchronized byte finishInsert(boolean success) {
        assert state == STATE_NEW;

        byte s = success ? STATE_INSERTED : STATE_REJECTED;

        state = s;

        notifyAll();

        return s;
    }

    /** {@inheritDoc} */
    @Override public long pointer() {
        throw new IllegalStateException();
    }

    /** {@inheritDoc} */
    @Override public void incrementRefCount() {
        throw new IllegalStateException();
    }

    /** {@inheritDoc} */
    @Override public void decrementRefCount() {
        throw new IllegalStateException();
    }
}
