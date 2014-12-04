/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.lang.*;
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
 */
public class GridCacheTxProxyImpl<K, V> implements GridCacheTxProxy, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Wrapped transaction. */
    @GridToStringInclude
    private GridCacheTxEx<K, V> tx;

    /** Gateway. */
    @GridToStringExclude
    private GridCacheSharedContext<K, V> cctx;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridCacheTxProxyImpl() {
        // No-op.
    }

    /**
     * @param tx Transaction.
     */
    public GridCacheTxProxyImpl(GridCacheTxEx<K, V> tx, GridCacheSharedContext<K, V> cctx) {
        assert tx != null;
        assert cctx != null;

        this.tx = tx;
        this.cctx = cctx;
    }

    /**
     * Enters a call.
     */
    private void enter() {
        if (cctx.deploymentEnabled())
            cctx.deploy().onEnter();

        try {
            cctx.kernalContext().gateway().readLock();
        }
        catch (IllegalStateException e) {
            throw e;
        }
        catch (RuntimeException | Error e) {
            cctx.kernalContext().gateway().readUnlock();

            throw e;
        }
    }

    /**
     * Leaves a call.
     */
    private void leave() {
        try {
            CU.unwindEvicts(cctx);
        }
        finally {
            cctx.kernalContext().gateway().readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid xid() {
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
            cctx.commitTxAsync(tx).get();
        }
        finally {
            leave();
        }
    }

    /** {@inheritDoc} */
    @Override public void close() throws GridException {
        enter();

        try {
            cctx.endTx(tx);
        }
        finally {
            leave();
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<GridCacheTx> commitAsync() {
        enter();

        try {

            return cctx.commitTxAsync(tx);
        }
        finally {
            leave();
        }
    }

    /** {@inheritDoc} */
    @Override public void rollback() throws GridException {
        enter();

        try {
            cctx.rollbackTx(tx);
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
    @Override public <V1> V1 addMeta(String name, V1 val) {
        return tx.addMeta(name, val);
    }

    /** {@inheritDoc} */
    @Override public <V1> V1 putMetaIfAbsent(String name, V1 val) {
        return tx.putMetaIfAbsent(name, val);
    }

    /** {@inheritDoc} */
    @Override public <V1> V1 putMetaIfAbsent(String name, Callable<V1> c) {
        return tx.putMetaIfAbsent(name, c);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <V1> V1 addMetaIfAbsent(String name, V1 val) {
        return tx.addMeta(name, val);
    }

    /** {@inheritDoc} */
    @Override public <V1> V1 addMetaIfAbsent(String name, @Nullable Callable<V1> c) {
        return tx.addMetaIfAbsent(name, c);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"RedundantTypeArguments"})
    @Override public <V1> V1 meta(String name) {
        return tx.<V1>meta(name);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"RedundantTypeArguments"})
    @Override public <V1> V1 removeMeta(String name) {
        return tx.<V1>removeMeta(name);
    }

    /** {@inheritDoc} */
    @Override public <V1> boolean removeMeta(String name, V1 val) {
        return tx.removeMeta(name, val);
    }

    /** {@inheritDoc} */
    @Override public <V1> Map<String, V1> allMeta() {
        return tx.allMeta();
    }

    /** {@inheritDoc} */
    @Override public boolean hasMeta(String name) {
        return tx.hasMeta(name);
    }

    /** {@inheritDoc} */
    @Override public <V1> boolean hasMeta(String name, V1 val) {
        return tx.hasMeta(name, val);
    }

    /** {@inheritDoc} */
    @Override public <V1> boolean replaceMeta(String name, V1 curVal, V1 newVal) {
        return tx.replaceMeta(name, curVal, newVal);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(tx);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        tx = (GridCacheTxAdapter<K, V>)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheTxProxyImpl.class, this);
    }
}
