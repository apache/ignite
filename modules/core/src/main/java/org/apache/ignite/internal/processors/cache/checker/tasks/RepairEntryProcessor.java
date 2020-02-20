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

package org.apache.ignite.internal.processors.cache.checker.tasks;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.checker.objects.VersionedValue;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_CACHE_REMOVED_ENTRIES_TTL;

/** Entry processor to repair inconsistent entries. */
public class RepairEntryProcessor implements EntryProcessor {
    /** Value to set. */
    private Object val;

    /** Map of nodes to corresponding versioned values */
    private Map<UUID, VersionedValue> data;

    /** deferred delete queue max size. */
    private long rmvQueueMaxSize;

    /** Force repair flag. */
    private boolean forceRepair;

    /** Start topology version. */
    private AffinityTopologyVersion startTopVer;

    /**
     * Describe result of reparation.
     */
    public enum RepairStatus {
        /**
         * Value changed.
         */
        SUCCESS,

        /**
         * Fail, not enough information for modification.
         */
        FAIL,

        /**
         * Value was modified from other thread. Result same the success.
         */
        CONCURRENT_MODIFICATION
    }

    /**
     * @param val Value.
     * @param data Data.
     * @param rmvQueueMaxSize Remove queue max size.
     * @param forceRepair Force repair.
     * @param startTopVer Start topology version.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public RepairEntryProcessor(
        Object val,
        Map<UUID, VersionedValue> data,
        long rmvQueueMaxSize,
        boolean forceRepair,
        AffinityTopologyVersion startTopVer) {
        this.val = val;
        this.data = data;
        this.rmvQueueMaxSize = rmvQueueMaxSize;
        this.forceRepair = forceRepair;
        this.startTopVer = startTopVer;
    }

    /**
     * Do repair logic.
     *
     * @param entry Entry to fix.
     * @param arguments Arguments.
     * @return {@link RepairStatus} looks at description of this class.
     * @throws EntryProcessorException If failed.
     */
    @SuppressWarnings("unchecked")
    @Override public Object process(MutableEntry entry, Object... arguments) throws EntryProcessorException {
        GridCacheContext cctx = cacheContext(entry);
        GridCacheVersion currKeyGridCacheVer = keyVersion(entry);

        if (topologyChanged(cctx, startTopVer))
            throw new EntryProcessorException("Topology version was changed");

        UUID locNodeId = cctx.localNodeId();
        VersionedValue versionedVal = data.get(locNodeId);

        if (versionedVal != null) {
            if (currKeyGridCacheVer.compareTo(versionedVal.version()) == 0) {
                if (val == null)
                    entry.remove();
                else
                    entry.setValue(val);

                return RepairStatus.SUCCESS;
            }
            else
                return RepairStatus.CONCURRENT_MODIFICATION;
        }
        else {
            if (currKeyGridCacheVer.compareTo(new GridCacheVersion(0, 0, 0)) == 0) {
                long recheckStartTime = minValue(VersionedValue::recheckStartTime);

                boolean inEntryTTLBounds =
                    (System.currentTimeMillis() - recheckStartTime) < Long.getLong(IGNITE_CACHE_REMOVED_ENTRIES_TTL, 10_000);

                // Min available update counter for the key at all nodes.
                // It just fast solution for null value problem. We should use other way to fix it (versionedVal.updateCounter()).
                long minUpdateCntr = minValue(VersionedValue::updateCounter);
                long currUpdateCntr = updateCounter(cctx, entry.getKey());

                boolean inDeferredDelQueueBounds = ((currUpdateCntr - minUpdateCntr) < rmvQueueMaxSize);

                // Remove it after fixes: https://ggsystems.atlassian.net/browse/GG-27419
                if (cctx.config().getAtomicityMode() != CacheAtomicityMode.ATOMIC || inEntryTTLBounds && inDeferredDelQueueBounds) {
                    if (val == null)
                        entry.remove();
                    else
                        entry.setValue(val);

                    return RepairStatus.SUCCESS;
                }
            }
            else
                return RepairStatus.CONCURRENT_MODIFICATION;

            if (forceRepair) {
                if (val == null)
                    entry.remove();
                else
                    entry.setValue(val);

                return RepairStatus.SUCCESS;
            }

            return RepairStatus.FAIL;
        }
    }

    /**
     *
     */
    protected GridCacheContext cacheContext(MutableEntry entry) {
        return (GridCacheContext)entry.unwrap(GridCacheContext.class);
    }

    /**
     *
     */
    protected boolean topologyChanged(GridCacheContext cctx, AffinityTopologyVersion expTop) {
        AffinityTopologyVersion currTopVer = cctx.affinity().affinityTopologyVersion();

        return !cctx.shared().exchange().lastAffinityChangedTopologyVersion((currTopVer)).equals(expTop);
    }

    /**
     * @return Current {@link GridCacheVersion}
     */
    protected GridCacheVersion keyVersion(MutableEntry entry) {
        CacheEntry verEntry = (CacheEntry)entry.unwrap(CacheEntry.class);

        return (GridCacheVersion)verEntry.version();
    }

    /**
     * @return Current update counter
     */
    protected long updateCounter(GridCacheContext cctx, Object affKey) {
        return cctx.topology().localPartition(cctx.cache().affinity().partition(affKey)).updateCounter();
    }

    /**
     * @return target min long value
     */
    private long minValue(Function<VersionedValue, Long> mapper) {
        return data.values().stream()
            .mapToLong(mapper::apply)
            .min()
            .orElseThrow(() -> new IllegalStateException("Unreachable state [mapper = " + mapper.getClass().getName() + ", data=" + data + "]."));
    }
}
