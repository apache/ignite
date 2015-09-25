package org.apache.ignite.internal.processors.datastructures;

import org.apache.ignite.internal.processors.cache.GridCacheInternal;
import org.apache.ignite.internal.util.typedef.internal.S;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by vladisav on 20.9.15..
 */
public class GridCacheSemaphoreValue implements GridCacheInternal, Externalizable, Cloneable {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Permission count.
     */
    private int cnt;

    /**
     * Semaphore ID.
     */
    private long semaphoreId;

    /**
     * Constructor.
     *
     * @param cnt Number of permissions.
     * @param
     */
    public GridCacheSemaphoreValue(int cnt, long semaphoreId) {
        this.cnt = cnt;

        this.semaphoreId = semaphoreId;
    }

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridCacheSemaphoreValue() {
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
     * @return true if number of permissions to be added is positive
     */
    public boolean isRelease(){
        return cnt>0;
    }

    /**
     * @return true if permission count should be lowered
     */
    public boolean isAwait(){
        return cnt<0;
    }

    /**
     * @return Semaphore ID.
     */
    public long semaphoreId() {
        return semaphoreId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(cnt);
        out.writeLong(semaphoreId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readExternal(ObjectInput in) throws IOException {
        cnt = in.readInt();
        semaphoreId = in.readLong();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return S.toString(GridCacheSemaphoreValue.class, this);
    }
}