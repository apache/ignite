/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.nio;

import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.concurrent.atomic.*;

/**
 * Implements basic lifecycle for communication clients.
 */
public abstract class GridAbstractCommunicationClient implements GridCommunicationClient {
    /** Time when this client was last used. */
    private volatile long lastUsed = U.currentTimeMillis();

    /** Reservations. */
    private final AtomicInteger reserves = new AtomicInteger();

    /** Metrics listener. */
    protected final GridNioMetricsListener metricsLsnr;

    /**
     * @param metricsLsnr Metrics listener.
     */
    protected GridAbstractCommunicationClient(@Nullable GridNioMetricsListener metricsLsnr) {
        this.metricsLsnr = metricsLsnr;
    }

    /** {@inheritDoc} */
    @Override public boolean close() {
        return reserves.compareAndSet(0, -1);
    }

    /** {@inheritDoc} */
    @Override public void forceClose() {
        reserves.set(-1);
    }

    /** {@inheritDoc} */
    @Override public boolean closed() {
        return reserves.get() == -1;
    }

    /** {@inheritDoc} */
    @Override public boolean reserve() {
        while (true) {
            int r = reserves.get();

            if (r == -1)
                return false;

            if (reserves.compareAndSet(r, r + 1))
                return true;
        }
    }

    /** {@inheritDoc} */
    @Override public void release() {
        while (true) {
            int r = reserves.get();

            if (r == -1)
                return;

            if (reserves.compareAndSet(r, r - 1))
                return;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean reserved() {
        return reserves.get() > 0;
    }

    /** {@inheritDoc} */
    @Override public long getIdleTime() {
        return U.currentTimeMillis() - lastUsed;
    }

    /**
     * Updates used time.
     */
    protected void markUsed() {
        lastUsed = U.currentTimeMillis();
    }

    /** {@inheritDoc} */
    @Override public boolean async() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridAbstractCommunicationClient.class, this);
    }
}
