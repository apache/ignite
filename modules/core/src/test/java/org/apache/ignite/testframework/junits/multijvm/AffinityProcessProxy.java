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

package org.apache.ignite.testframework.junits.multijvm;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgniteCallable;
import org.jetbrains.annotations.Nullable;

/**
 * Proxy class for affinity at another JVM.
 */
@SuppressWarnings("TransientFieldInNonSerializableClass")
public class AffinityProcessProxy<K> implements Affinity<K> {
    /** Compute. */
    private final transient IgniteCompute compute;

    /** Cache name. */
    private final String cacheName;

    /** Grid id. */
    private final UUID gridId;

    /**
     * @param cacheName Cache name.
     * @param proxy Ignite ptocess proxy.
     */
    public AffinityProcessProxy(String cacheName, IgniteProcessProxy proxy) {
        this.cacheName = cacheName;
        gridId = proxy.getId();
        compute = proxy.remoteCompute();
    }

    /**
     * Returns cache instance. Method to be called from closure at another JVM.
     *
     * @return Cache.
     */
    private Affinity<Object> affinity() {
        return Ignition.ignite(gridId).affinity(cacheName);
    }

    /** {@inheritDoc} */
    @Override public int partitions() {
        return (int)compute.call(new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                return affinity().partitions();
            }
        });
    }

    /** {@inheritDoc} */
    @Override public int partition(final K key) {
        return (int)compute.call(new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                return affinity().partition(key);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public boolean isPrimary(final ClusterNode n, final K key) {
        return (boolean)compute.call(new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                return affinity().isPrimary(n, key);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public boolean isBackup(final ClusterNode n, final K key) {
        return (boolean)compute.call(new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                return affinity().isBackup(n, key);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public boolean isPrimaryOrBackup(final ClusterNode n, final K key) {
        return (boolean)compute.call(new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                return affinity().isPrimaryOrBackup(n, key);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public int[] primaryPartitions(final ClusterNode n) {
        return (int[])compute.call(new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                return affinity().primaryPartitions(n);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public int[] backupPartitions(final ClusterNode n) {
        return (int[])compute.call(new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                return affinity().backupPartitions(n);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public int[] allPartitions(final ClusterNode n) {
        return (int[])compute.call(new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                return affinity().allPartitions(n);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public Object affinityKey(final K key) {
        return compute.call(new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                return affinity().affinityKey(key);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public Map<ClusterNode, Collection<K>> mapKeysToNodes(final Collection<? extends K> keys) {
        return (Map<ClusterNode, Collection<K>>)compute.call(new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                return affinity().mapKeysToNodes(keys);
            }
        });
    }

    /** {@inheritDoc} */
    @Nullable @Override public ClusterNode mapKeyToNode(final K key) {
        return (ClusterNode)compute.call(new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                return affinity().mapKeyToNode(key);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> mapKeyToPrimaryAndBackups(final K key) {
        return (Collection<ClusterNode>)compute.call(new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                return affinity().mapKeyToPrimaryAndBackups(key);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public ClusterNode mapPartitionToNode(final int part) {
        return (ClusterNode)compute.call(new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                return affinity().mapPartitionToNode(part);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public Map<Integer, ClusterNode> mapPartitionsToNodes(final Collection<Integer> parts) {
        return (Map<Integer, ClusterNode>)compute.call(new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                return affinity().mapPartitionsToNodes(parts);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> mapPartitionToPrimaryAndBackups(final int part) {
        return (Collection<ClusterNode>)compute.call(new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                return affinity().mapPartitionToPrimaryAndBackups(part);
            }
        });
    }
}