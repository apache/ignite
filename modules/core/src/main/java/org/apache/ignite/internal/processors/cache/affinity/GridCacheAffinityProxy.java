/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.affinity;

import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.processors.cache.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Affinity interface implementation.
 */
public class GridCacheAffinityProxy<K, V> implements Affinity<K>, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Cache gateway. */
    private GridCacheGateway<K, V> gate;

    /** Affinity delegate. */
    private Affinity<K> delegate;

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
    public GridCacheAffinityProxy(GridCacheContext<K, V> cctx, Affinity<K> delegate) {
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
    @Override public boolean isPrimary(ClusterNode n, K key) {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.isPrimary(n, key);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isBackup(ClusterNode n, K key) {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.isBackup(n, key);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isPrimaryOrBackup(ClusterNode n, K key) {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.isPrimaryOrBackup(n, key);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public int[] primaryPartitions(ClusterNode n) {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.primaryPartitions(n);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public int[] backupPartitions(ClusterNode n) {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.backupPartitions(n);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public int[] allPartitions(ClusterNode n) {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.allPartitions(n);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public ClusterNode mapPartitionToNode(int part) {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.mapPartitionToNode(part);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public Map<Integer, ClusterNode> mapPartitionsToNodes(Collection<Integer> parts) {
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
    @Override @Nullable public ClusterNode mapKeyToNode(K key) {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.mapKeyToNode(key);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public Map<ClusterNode, Collection<K>> mapKeysToNodes(@Nullable Collection<? extends K> keys) {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.mapKeysToNodes(keys);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> mapKeyToPrimaryAndBackups(K key) {
        GridCacheProjectionImpl<K, V> old = gate.enter(null);

        try {
            return delegate.mapKeyToPrimaryAndBackups(key);
        }
        finally {
            gate.leave(old);
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> mapPartitionToPrimaryAndBackups(int part) {
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
        return cctx.grid().affinity(cctx.cache().name());
    }
}
