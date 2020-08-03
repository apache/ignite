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
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;
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

    /**
     * @param cacheName Cache name.
     * @param proxy Ignite process proxy.
     */
    public AffinityProcessProxy(String cacheName, IgniteProcessProxy proxy) {
        this.cacheName = cacheName;
        this.compute = proxy.remoteCompute();
    }

    /** {@inheritDoc} */
    @Override public int partitions() {
        return compute.call(new PartitionsTask(cacheName));
    }

    /** {@inheritDoc} */
    @Override public int partition(K key) {
        return compute.call(new PartitionTask<>(cacheName, key));
    }

    /** {@inheritDoc} */
    @Override public boolean isPrimary(ClusterNode n, K key) {
        return compute.call(new PrimaryOrBackupNodeTask<>(cacheName, key, n, true, false));
    }

    /** {@inheritDoc} */
    @Override public boolean isBackup(ClusterNode n, K key) {
        return compute.call(new PrimaryOrBackupNodeTask<>(cacheName, key, n, false, true));
    }

    /** {@inheritDoc} */
    @Override public boolean isPrimaryOrBackup(ClusterNode n, K key) {
        return compute.call(new PrimaryOrBackupNodeTask<>(cacheName, key, n, true, true));
    }

    /** {@inheritDoc} */
    @Override public int[] primaryPartitions(ClusterNode n) {
        return compute.call(new GetPartitionsTask(cacheName, n, true, false));
    }

    /** {@inheritDoc} */
    @Override public int[] backupPartitions(ClusterNode n) {
        return compute.call(new GetPartitionsTask(cacheName, n, false, true));
    }

    /** {@inheritDoc} */
    @Override public int[] allPartitions(ClusterNode n) {
        return compute.call(new GetPartitionsTask(cacheName, n, true, true));
    }

    /** {@inheritDoc} */
    @Override public Object affinityKey(K key) {
        return compute.call(new AffinityKeyTask<>(cacheName, key));
    }

    /** {@inheritDoc} */
    @Override public Map<ClusterNode, Collection<K>> mapKeysToNodes(Collection<? extends K> keys) {
        return compute.call(new MapKeysToNodesTask<>(cacheName, keys));
    }

    /** {@inheritDoc} */
    @Nullable @Override public ClusterNode mapKeyToNode(K key) {
        return compute.call(new MapKeyToNodeTask<>(cacheName, key));
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> mapKeyToPrimaryAndBackups(K key) {
        return compute.call(new MapKeyToPrimaryAndBackupsTask<>(cacheName, key));
    }

    /** {@inheritDoc} */
    @Override public ClusterNode mapPartitionToNode(int part) {
        return compute.call(new MapPartitionToNode<>(cacheName, part));
    }

    /** {@inheritDoc} */
    @Override public Map<Integer, ClusterNode> mapPartitionsToNodes(Collection<Integer> parts) {
        return compute.call(new MapPartitionsToNodes<>(cacheName, parts));
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> mapPartitionToPrimaryAndBackups(int part) {
        return compute.call(new MapPartitionsToPrimaryAndBackupsTask<>(cacheName, part));
    }

    /**
     *
     */
    private static class PrimaryOrBackupNodeTask<K> extends AffinityTaskAdapter<K, Boolean> {
        /** Key. */
        private final K key;

        /** Node. */
        private final ClusterNode n;

        /** Primary. */
        private final boolean primary;

        /** Backup. */
        private final boolean backup;

        /**
         * @param cacheName Cache name.
         * @param key Key.
         * @param n N.
         */
        public PrimaryOrBackupNodeTask(String cacheName, K key, ClusterNode n,
            boolean primary, boolean backup) {
            super(cacheName);
            this.key = key;
            this.n = n;
            this.primary = primary;
            this.backup = backup;
        }

        /** {@inheritDoc} */
        @Override public Boolean call() throws Exception {
            if (primary && backup)
                return affinity().isPrimaryOrBackup(n, key);
            else if (primary)
                return affinity().isPrimary(n, key);
            else if (backup)
                return affinity().isBackup(n, key);
            else
                throw new IllegalStateException("primary or backup or both flags should be switched on");
        }
    }

    /**
     *
     */
    private static class MapKeyToPrimaryAndBackupsTask<K> extends AffinityTaskAdapter<K, Collection<ClusterNode>> {
        /** Key. */
        private final K key;

        /**
         * @param cacheName Cache name.
         * @param key Key.
         */
        public MapKeyToPrimaryAndBackupsTask(String cacheName, K key) {
            super(cacheName);
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public Collection<ClusterNode> call() throws Exception {
            return affinity().mapKeyToPrimaryAndBackups(key);
        }
    }

    /**
     *
     */
    private static class PartitionsTask extends AffinityTaskAdapter<Void, Integer> {
        /**
         * @param cacheName Cache name.
         */
        public PartitionsTask(String cacheName) {
            super(cacheName);
        }

        /** {@inheritDoc} */
        @Override public Integer call() throws Exception {
            return affinity().partitions();
        }
    }

    /**
     *
     */
    private static class PartitionTask<K> extends AffinityTaskAdapter<K, Integer> {
        /** Key. */
        private final K key;

        /**
         * @param cacheName Cache name.
         * @param key Key.
         */
        public PartitionTask(String cacheName, K key) {
            super(cacheName);
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public Integer call() throws Exception {
            return affinity().partition(key);
        }
    }

    /**
     *
     */
    private static class GetPartitionsTask extends AffinityTaskAdapter<Void, int[]> {
        /** Node. */
        private final ClusterNode n;

        /** Primary. */
        private final boolean primary;

        /** Backup. */
        private final boolean backup;

        /**
         * @param cacheName Cache name.
         * @param n N.
         * @param primary Primary.
         * @param backup Backup.
         */
        public GetPartitionsTask(String cacheName, ClusterNode n, boolean primary, boolean backup) {
            super(cacheName);
            this.n = n;
            this.primary = primary;
            this.backup = backup;
        }

        /** {@inheritDoc} */
        @Override public int[] call() throws Exception {
            if (primary && backup)
                return affinity().allPartitions(n);
            else if (primary)
                return affinity().primaryPartitions(n);
            else if (backup)
                return affinity().backupPartitions(n);
            else
                throw new IllegalStateException("primary or backup or both flags should be switched on");
        }
    }

    /**
     *
     */
    private static class AffinityKeyTask<K> extends AffinityTaskAdapter<K, Object> {
        /** Key. */
        private final K key;

        /**
         * @param cacheName Cache name.
         * @param key Key.
         */
        public AffinityKeyTask(String cacheName, K key) {
            super(cacheName);
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public Object call() throws Exception {
            return affinity().affinityKey(key);
        }
    }

    /**
     * @param <K>
     */
    private static class MapKeysToNodesTask<K> extends AffinityTaskAdapter<K, Map<ClusterNode, Collection<K>>> {
        /** Keys. */
        private final Collection<? extends K> keys;

        /**
         * @param cacheName Cache name.
         * @param keys Keys.
         */
        public MapKeysToNodesTask(String cacheName, Collection<? extends K> keys) {
            super(cacheName);
            this.keys = keys;
        }

        /** {@inheritDoc} */
        @Override public Map<ClusterNode, Collection<K>> call() throws Exception {
            return affinity().mapKeysToNodes(keys);
        }
    }

    /**
     *
     */
    private static class MapKeyToNodeTask<K> extends AffinityTaskAdapter<K, ClusterNode> {
        /** Key. */
        private final K key;

        /**
         * @param cacheName Cache name.
         * @param key Key.
         */
        public MapKeyToNodeTask(String cacheName, K key) {
            super(cacheName);
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public ClusterNode call() throws Exception {
            return affinity().mapKeyToNode(key);
        }
    }

    /**
     *
     */
    private static class MapPartitionToNode<K> extends AffinityTaskAdapter<K, ClusterNode> {
        /** Partition. */
        private final int part;

        /**
         * @param cacheName Cache name.
         * @param part Partition.
         */
        public MapPartitionToNode(String cacheName, int part) {
            super(cacheName);
            this.part = part;
        }

        /** {@inheritDoc} */
        @Override public ClusterNode call() throws Exception {
            return affinity().mapPartitionToNode(part);
        }
    }

    /**
     *
     */
    private static class MapPartitionsToNodes<K> extends AffinityTaskAdapter<K, Map<Integer, ClusterNode>> {
        /** Parts. */
        private final Collection<Integer> parts;

        /**
         * @param cacheName Cache name.
         * @param parts Parts.
         */
        public MapPartitionsToNodes(String cacheName, Collection<Integer> parts) {
            super(cacheName);
            this.parts = parts;
        }

        /** {@inheritDoc} */
        @Override public Map<Integer, ClusterNode> call() throws Exception {
            return affinity().mapPartitionsToNodes(parts);
        }
    }

    /**
     *
     */
    private static class MapPartitionsToPrimaryAndBackupsTask<K> extends AffinityTaskAdapter<K, Collection<ClusterNode>> {
        /** Partition. */
        private final int part;

        /**
         * @param cacheName Cache name.
         * @param part Partition.
         */
        public MapPartitionsToPrimaryAndBackupsTask(String cacheName, int part) {
            super(cacheName);
            this.part = part;
        }

        /** {@inheritDoc} */
        @Override public Collection<ClusterNode> call() throws Exception {
            return affinity().mapPartitionToPrimaryAndBackups(part);
        }
    }

    /**
     *
     */
    private abstract static class AffinityTaskAdapter<K, R> implements IgniteCallable<R> {
        /** Ignite. */
        @IgniteInstanceResource
        protected Ignite ignite;

        /** Cache name. */
        protected final String cacheName;

        /**
         * @param cacheName Cache name.
         */
        public AffinityTaskAdapter(String cacheName) {
            this.cacheName = cacheName;
        }

        /**
         * @return Affinity.
         */
        protected Affinity<K> affinity() {
            return ignite.affinity(cacheName);
        }
    }
}
