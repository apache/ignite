/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Cache transaction proxy.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridCacheTxProxyImpl<K, V> implements GridCacheTxProxy, Externalizable {
    /** Wrapped transaction. */
    @GridToStringInclude
    private GridCacheTx tx;

    /** Gateway. */
    @GridToStringExclude
    private GridCacheGateway gate;

    /** Cache adapter. */
    @GridToStringExclude
    private GridCacheAdapter<K, V> cache;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridCacheTxProxyImpl() {
        // No-op.
    }

    /**
     * @param tx Transaction.
     * @param gate Gateway.
     */
    public GridCacheTxProxyImpl(GridCacheTx tx, GridCacheAdapter<K, V> cache, GridCacheGateway gate) {
        assert tx != null;
        assert gate != null;
        assert cache != null;

        this.tx = tx;
        this.cache = cache;
        this.gate = gate;
    }

    /**
     * Enters a call.
     */
    private void enter() {
        if (gate != null)
            gate.enter();
    }

    /**
     * Leaves a call.
     */
    private void leave() {
        if (gate != null)
            gate.leave();
    }

    /** {@inheritDoc} */
    @Override public GridUuid xid() {
        return tx.xid();
    }

    /** {@inheritDoc} */
    @Override public UUID nodeId() {
        return tx.nodeId();
    }

    /** {@inheritDoc} */
    @Override public long threadId() {
        return tx.threadId();
    }

    /** {@inheritDoc} */
    @Override public long startTime() {
        return tx.startTime();
    }

    /** {@inheritDoc} */
    @Override public GridCacheTxIsolation isolation() {
        return tx.isolation();
    }

    /** {@inheritDoc} */
    @Override public GridCacheTxConcurrency concurrency() {
        return tx.concurrency();
    }

    /** {@inheritDoc} */
    @Override public boolean isInvalidate() {
        return tx.isInvalidate();
    }

    /** {@inheritDoc} */
    @Override public boolean implicit() {
        return tx.implicit();
    }

    /** {@inheritDoc} */
    @Override public long timeout() {
        return tx.timeout();
    }

    /** {@inheritDoc} */
    @Override public GridCacheTxState state() {
        return tx.state();
    }

    /** {@inheritDoc} */
    @Override public long timeout(long timeout) {
        return tx.timeout(timeout);
    }

    /** {@inheritDoc} */
    @Override public boolean setRollbackOnly() {
        enter();

        try {
            return tx.setRollbackOnly();
        }
        finally {
            leave();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isRollbackOnly() {
        enter();

        try {
            return tx.isRollbackOnly();
        }
        finally {
            leave();
        }
    }

    /** {@inheritDoc} */
    @Override public void commit() throws GridException {
        enter();

        try {
            tx.commit();
        }
        finally {
            leave();
        }
    }

    /** {@inheritDoc} */
    @Override public void close() throws GridException {
        enter();

        try {
            cache.endTx(tx);
        }
        finally {
            leave();
        }
    }

    /** {@inheritDoc} */
    @Override public GridFuture<GridCacheTx> commitAsync() {
        enter();

        try {
            return cache.commitTxAsync(tx);
        }
        finally {
            leave();
        }
    }

    /** {@inheritDoc} */
    @Override public void rollback() throws GridException {
        enter();

        try {
            cache.rollbackTx(tx);
        }
        finally {
            leave();
        }
    }

    /** {@inheritDoc} */
    @Override public void copyMeta(GridMetadataAware from) {
        tx.copyMeta(from);
    }

    /** {@inheritDoc} */
    @Override public void copyMeta(Map<String, ?> data) {
        tx.copyMeta(data);
    }

    /** {@inheritDoc} */
    @Override public <V> V addMeta(String name, V val) {
        return tx.addMeta(name, val);
    }

    /** {@inheritDoc} */
    @Override public <V> V putMetaIfAbsent(String name, V val) {
        return tx.putMetaIfAbsent(name, val);
    }

    /** {@inheritDoc} */
    @Override public <V> V putMetaIfAbsent(String name, Callable<V> c) {
        return tx.putMetaIfAbsent(name, c);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <V> V addMetaIfAbsent(String name, V val) {
        return tx.addMeta(name, val);
    }

    /** {@inheritDoc} */
    @Override public <V> V addMetaIfAbsent(String name, @Nullable Callable<V> c) {
        return tx.addMetaIfAbsent(name, c);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"RedundantTypeArguments"})
    @Override public <V> V meta(String name) {
        return tx.<V>meta(name);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"RedundantTypeArguments"})
    @Override public <V> V removeMeta(String name) {
        return tx.<V>removeMeta(name);
    }

    /** {@inheritDoc} */
    @Override public <V> boolean removeMeta(String name, V val) {
        return tx.removeMeta(name, val);
    }

    /** {@inheritDoc} */
    @Override public <V> Map<String, V> allMeta() {
        return tx.allMeta();
    }

    /** {@inheritDoc} */
    @Override public boolean hasMeta(String name) {
        return tx.hasMeta(name);
    }

    /** {@inheritDoc} */
    @Override public <V> boolean hasMeta(String name, V val) {
        return tx.hasMeta(name, val);
    }

    /** {@inheritDoc} */
    @Override public <V> boolean replaceMeta(String name, V curVal, V newVal) {
        return tx.replaceMeta(name, curVal, newVal);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(tx);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        tx = (GridCacheTx)in.readObject();

        gate = null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheTxProxyImpl.class, this);
    }
}
