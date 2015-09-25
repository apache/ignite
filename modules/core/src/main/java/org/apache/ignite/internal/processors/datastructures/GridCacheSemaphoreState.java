package org.apache.ignite.internal.processors.datastructures;

import org.apache.ignite.internal.processors.cache.GridCacheInternal;
import org.apache.ignite.internal.util.typedef.internal.S;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


/**
 * Grid cache semaphore state.
 *
 * @author Vladisav Jelisavcic
 */
public class GridCacheSemaphoreState implements GridCacheInternal, Externalizable, Cloneable {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Permission count.
     */
    private int cnt;

    /**
     * Waiter id.
     */
    private int waiters;

    /**
     * Fairness flag.
     */
    private boolean fair;


    /**
     * Constructor.
     *
     * @param cnt Number of permissions.
     */
    public GridCacheSemaphoreState(int cnt, int waiters) {
        this.cnt = cnt;
        this.waiters = waiters;
        this.fair = false;
    }

    /**
     * Constructor.
     *
     * @param cnt Number of permissions.
     */
    public GridCacheSemaphoreState(int cnt, int waiters, boolean fair) {
        this.cnt = cnt;
        this.waiters = waiters;
        this.fair = fair;
    }

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridCacheSemaphoreState() {
        // No-op.
    }

    /**
     * @param cnt New count.
     */
    public void setCnt(int cnt) {
        this.cnt = cnt;
    }

    /**
     * @return Current count.
     */
    public int getCnt() {
        return cnt;
    }

    public int getWaiters() {
        return waiters;
    }

    public void setWaiters(int id) {
        this.waiters = id;
    }

    public boolean isFair() {
        return fair;
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
        out.writeInt(waiters);
        out.writeBoolean(fair);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readExternal(ObjectInput in) throws IOException {
        cnt = in.readInt();
        waiters = in.readInt();
        fair = in.readBoolean();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return S.toString(GridCacheSemaphoreState.class, this);
    }
}

