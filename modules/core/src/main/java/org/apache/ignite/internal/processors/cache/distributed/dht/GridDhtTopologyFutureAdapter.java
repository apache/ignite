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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.TopologyValidator;
import org.apache.ignite.internal.cluster.ClusterReadOnlyModeCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheInvalidStateException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.PartitionLossPolicy.READ_ONLY_ALL;
import static org.apache.ignite.cache.PartitionLossPolicy.READ_ONLY_SAFE;
import static org.apache.ignite.cache.PartitionLossPolicy.READ_WRITE_SAFE;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.isSystemCache;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFutureAdapter.OperationType.WRITE;

/**
 *
 */
public abstract class GridDhtTopologyFutureAdapter extends GridFutureAdapter<AffinityTopologyVersion>
    implements GridDhtTopologyFuture {
    /** Cache groups validation results. */
    protected volatile Map<Integer, CacheGroupValidation> grpValidRes = Collections.emptyMap();

    /** Whether or not cluster is active. */
    protected volatile boolean clusterIsActive = true;

    /**
     * @param grp Cache group.
     * @param topNodes Topology nodes.
     * @return Validation result.
     */
    protected final CacheGroupValidation validateCacheGroup(CacheGroupContext grp, Collection<ClusterNode> topNodes) {
        Collection<Integer> lostParts = grp.isLocal() ?
            Collections.<Integer>emptyList() : grp.topology().lostPartitions();

        boolean valid = true;

        if (!grp.systemCache()) {
            TopologyValidator validator = grp.topologyValidator();

            if (validator != null)
                valid = validator.validate(topNodes);
        }

        return new CacheGroupValidation(valid, lostParts);
    }

    /** {@inheritDoc} */
    @Override public final @Nullable Throwable validateCache(
        GridCacheContext cctx,
        boolean recovery,
        boolean read,
        @Nullable Object key,
        @Nullable Collection<?> keys
    ) {
        assert isDone() : this;

        Throwable err = error();

        if (err != null)
            return err;

        if (!clusterIsActive)
            return new CacheInvalidStateException(
                "Failed to perform cache operation (cluster is not activated): " + cctx.name());

        if (cctx.cache() == null)
            return new CacheInvalidStateException(
                "Failed to perform cache operation (cache is stopped): " + cctx.name());

        OperationType opType = read ? OperationType.READ : WRITE;

        CacheGroupContext grp = cctx.group();

        PartitionLossPolicy lossPlc = grp.config().getPartitionLossPolicy();

        if (cctx.shared().readOnlyMode() && opType == WRITE && !isSystemCache(cctx.name()))
            return new ClusterReadOnlyModeCheckedException("Failed to perform cache operation (cluster is in read-only mode)");

        if (grp.needsRecovery() && !recovery) {
            if (opType == WRITE && (lossPlc == READ_ONLY_SAFE || lossPlc == READ_ONLY_ALL))
                return new IgniteCheckedException(
                    "Failed to write to cache (cache is moved to a read-only state): " + cctx.name());
        }

        CacheGroupValidation validation = grpValidRes.get(grp.groupId());

        if (validation == null)
            return null;

        if (opType == WRITE && !validation.isValid()) {
            return new IgniteCheckedException("Failed to perform cache operation " +
                "(cache topology is not valid): " + cctx.name());
        }

        if (recovery)
            return null;

        if (validation.hasLostPartitions()) {
            if (key != null)
                return LostPolicyValidator.validate(cctx, key, opType, validation.lostPartitions());

            if (keys != null)
                return LostPolicyValidator.validate(cctx, keys, opType, validation.lostPartitions());
        }

        return null;
    }

    /**
     * @return {@code true} If any lost partitions was detected.
     */
    public boolean hasLostPartitions() {
        return grpValidRes.values().stream()
            .anyMatch(CacheGroupValidation::hasLostPartitions);
    }

    /**
     * Cache group validation result.
     */
    protected static class CacheGroupValidation {
        /** Topology validation result. */
        private final boolean valid;

        /** Lost partitions on this topology version. */
        private final Collection<Integer> lostParts;

        /**
         * @param valid Valid flag.
         * @param lostParts Lost partitions.
         */
        private CacheGroupValidation(boolean valid, Collection<Integer> lostParts) {
            this.valid = valid;
            this.lostParts = lostParts;
        }

        /**
         * @return True if valid, False if invalide.
         */
        public boolean isValid() {
            return valid;
        }

        /**
         * @return True if lost partition is present, False if not.
         */
        public boolean hasLostPartitions() {
            return !F.isEmpty(lostParts);
        }

        /**
         * @return Lost patition ID collection.
         */
        public Collection<Integer> lostPartitions() {
            return lostParts;
        }
    }

    /**
     *
     */
    public enum OperationType {
        /**
         * Read operation.
         */
        READ,
        /**
         * Write operation.
         */
        WRITE
    }

    /**
     * Lost policy validator.
     */
    public static class LostPolicyValidator {
        /**
         *
         */
        public static Throwable validate(
            GridCacheContext cctx,
            Object key,
            OperationType opType,
            Collection<Integer> lostParts
        ) {
            CacheGroupContext grp = cctx.group();

            PartitionLossPolicy lostPlc = grp.config().getPartitionLossPolicy();

            int partition = cctx.affinity().partition(key);

            return validate(cctx, key, partition, opType, lostPlc, lostParts);
        }

        /**
         *
         */
        public static Throwable validate(
            GridCacheContext cctx,
            Collection<?> keys,
            OperationType opType,
            Collection<Integer> lostParts
        ) {
            CacheGroupContext grp = cctx.group();

            PartitionLossPolicy lostPlc = grp.config().getPartitionLossPolicy();

            for (Object key : keys) {
                int partition = cctx.affinity().partition(key);

                Throwable res = validate(cctx, key, partition, opType, lostPlc, lostParts);

                if (res != null)
                    return res;
            }

            return null;
        }

        /**
         *
         */
        private static Throwable validate(
            GridCacheContext cctx,
            Object key,
            int partition,
            OperationType opType,
            PartitionLossPolicy lostPlc,
            Collection<Integer> lostParts
        ) {
            if (opType == WRITE) {
                if (lostPlc == READ_ONLY_SAFE || lostPlc == READ_ONLY_ALL) {
                    return new IgniteCheckedException(
                        "Failed to write to cache (cache is moved to a read-only state): " + cctx.name()
                    );
                }

                if (lostParts.contains(partition) && lostPlc == READ_WRITE_SAFE) {
                    return new CacheInvalidStateException("Failed to execute cache operation " +
                        "(all partition owners have left the grid, partition data has been lost) [" +
                        "cacheName=" + cctx.name() + ", part=" + partition + ", key=" + key + ']');
                }
            }

            if (opType == OperationType.READ) {
                if (lostParts.contains(partition) && (lostPlc == READ_ONLY_SAFE || lostPlc == READ_WRITE_SAFE))
                    return new CacheInvalidStateException("Failed to execute cache operation " +
                        "(all partition owners have left the grid, partition data has been lost) [" +
                        "cacheName=" + cctx.name() + ", part=" + partition + ", key=" + key + ']'
                    );
            }

            return null;
        }
    }
}
