/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Count down latch value.
 */
public final class GridCacheCountDownLatchValue implements GridCacheInternal, Externalizable, Cloneable {
    private static final long serialVersionUID = 0L;

    /** Count. */
    private int cnt;

    /** Initial count. */
    private int initCnt;

    /** Auto delete flag. */
    private boolean autoDel;

    /**
     * Constructor.
     *
     * @param cnt Initial count.
     * @param del {@code True} to auto delete on count down to 0.
     */
    public GridCacheCountDownLatchValue(int cnt, boolean del) {
        assert cnt >= 0;

        this.cnt = cnt;

        initCnt = cnt;

        autoDel = del;
    }

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridCacheCountDownLatchValue() {
        // No-op.
    }

    /**
     * @param cnt New count.
     */
    public void set(int cnt) {
        this.cnt = cnt;
    }

    /**
     * @return Current count.
     */
    public int get() {
        return cnt;
    }

    /**
     * @return Initial count.
     */
    public int initialCount() {
        return initCnt;
    }

    /**
     * @return Auto-delete flag.
     */
    public boolean autoDelete() {
        return autoDel;
    }

    /** {@inheritDoc} */
    @Override public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(cnt);
        out.writeInt(initCnt);
        out.writeBoolean(autoDel);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException {
        cnt = in.readInt();
        initCnt = in.readInt();
        autoDel = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheCountDownLatchValue.class, this);
    }
}
