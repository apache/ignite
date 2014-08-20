/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.affinity;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Affinity interface implementation.
 */
public class GridCacheAffinityProxy<K, V> implements GridCacheAffinity<K>, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache gateway. */
    private GridCacheGateway<K, V> gate;

    /** Affinity delegate. */
    private GridCacheAffinity<K> delegate;

    /** Context. */
    private GridCacheContext<K, V> cctx;

    /**
     * Required by {@link Externalizable}.
     */
    public GridCacheAffinityProxy() {
        // No-op.
    }

    /**
     * @param cctx Context.
     * @param delegate Delegate object.
     */
    public GridCacheAffinityProxy(GridCacheContext<K, V> cctx, GridCacheAffinity<K> delegate) {
        gate = cctx.gate();
        this.delegate = delegate;
        this.cctx = cctx;
    }

    /** {@inheritDoc} */
    @Override public int partitions() {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.partitions();
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public int partition(K key) {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.partition(key);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isPrimary(GridNode n, K key) {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.isPrimary(n, key);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isBackup(GridNode n, K key) {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.isBackup(n, key);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isPrimaryOrBackup(GridNode n, K key) {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.isPrimaryOrBackup(n, key);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public int[] primaryPartitions(GridNode n) {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.primaryPartitions(n);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public int[] backupPartitions(GridNode n) {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.backupPartitions(n);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public int[] allPartitions(GridNode n) {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.allPartitions(n);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public GridNode mapPartitionToNode(int part) {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.mapPartitionToNode(part);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public Map<Integer, GridNode> mapPartitionsToNodes(Collection<Integer> parts) {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.mapPartitionsToNodes(parts);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public Object affinityKey(K key) {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.affinityKey(key);
        }
        finally {
            gate.leave(old);
        }
    }


    /** {@inheritDoc} */
    @Override @Nullable public GridNode mapKeyToNode(K key) {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.mapKeyToNode(key);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public Map<GridNode, Collection<K>> mapKeysToNodes(@Nullable Collection<? extends K> keys) {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.mapKeysToNodes(keys);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<GridNode> mapKeyToPrimaryAndBackups(K key) {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.mapKeyToPrimaryAndBackups(key);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<GridNode> mapPartitionToPrimaryAndBackups(int part) {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.mapPartitionToPrimaryAndBackups(part);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(cctx);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        cctx = (GridCacheContext<K, V>)in.readObject();
    }

    /**
     * Reconstructs object on unmarshalling.
     *
     * @return Reconstructed object.
     * @throws ObjectStreamException Thrown in case of unmarshalling error.
     */
    private Object readResolve() throws ObjectStreamException {
        return cctx.grid().cache(cctx.cache().name()).affinity();
    }
}
