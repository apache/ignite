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

package org.apache.ignite.internal.visor.consistency;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.CacheConsistencyViolationEvent;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.near.consistency.IgniteConsistencyViolationException;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.LoggerResource;

import static org.apache.ignite.events.EventType.EVT_CONSISTENCY_VIOLATION;

/**
 *
 */
public class VisorConsistencyRepairTask extends AbstractConsistencyTask<VisorConsistencyRepairTaskArg, String> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Nothing found. */
    public static final String NOTHING_FOUND = "Consistency violations were NOT found";

    /** Found. */
    public static final String CONSISTENCY_VIOLATIONS_FOUND = "Consistency violations were FOUND";

    /** Violations recorder. */
    public static final String CONSISTENCY_VIOLATIONS_RECORDED = "Cache consistency violations recorded.";

    /** {@inheritDoc} */
    @Override protected VisorJob<VisorConsistencyRepairTaskArg, String> job(VisorConsistencyRepairTaskArg arg) {
        return new VisorConsistencyRepairJob(arg, debug);
    }

    /**
     *
     */
    private static class VisorConsistencyRepairJob extends VisorJob<VisorConsistencyRepairTaskArg, String> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Injected logger. */
        @LoggerResource
        protected IgniteLogger log;

        /** Events. */
        private final Set<CacheConsistencyViolationEvent> evts = new GridConcurrentHashSet<>();

        /**
         * @param arg Arguments.
         * @param debug Debug.
         */
        protected VisorConsistencyRepairJob(VisorConsistencyRepairTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected String run(VisorConsistencyRepairTaskArg arg) throws IgniteException {
            String cacheName = arg.cacheName();
            int p = arg.part();
            int batchSize = 1024;
            int statusDelay = 60_000; // Every minute.

            IgniteInternalCache<Object, Object> internalCache = ignite.context().cache().cache(cacheName);

            if (internalCache == null)
                if (ignite.context().cache().cacheDescriptor(cacheName) != null)
                    return null; // Node filtered by node filter.
                else
                    throw new IgniteException("Cache not found [name=" + cacheName + "]");

            GridCacheContext<Object, Object> cctx = internalCache.context();

            if (!cctx.gridEvents().isRecordable(EVT_CONSISTENCY_VIOLATION))
                throw new UnsupportedOperationException("Consistency violation events recording is disabled on cluster.");

            CacheGroupContext grpCtx = cctx.group();

            GridDhtLocalPartition part = grpCtx.topology().localPartition(p);

            if (part == null)
                return null; // Partition does not belong to the node.

            log.info("Consistency check started [grp=" + grpCtx.cacheOrGroupName() + ", part=" + p + "]");

            VisorConsistencyStatusTask.MAP.put(arg, "0/" + part.fullSize());

            long cnt = 0;
            long statusTs = 0;

            part.reserve();

            try {
                IgnitePredicate<CacheConsistencyViolationEvent> lsnr = new CacheConsistencyViolationEventListener();

                ignite.events().localListen(lsnr, EVT_CONSISTENCY_VIOLATION);

                try {
                    Set<Object> keys = new HashSet<>();

                    GridCursor<? extends CacheDataRow> cursor = grpCtx.offheap().dataStore(part).cursor(cctx.cacheId());

                    IgniteCache<Object, Object> cache = ignite.cache(cacheName).withKeepBinary().withReadRepair();

                    do {
                        keys.clear();

                        for (int i = 0; i < batchSize && cursor.next(); i++) {
                            CacheDataRow row = cursor.get();

                            keys.add(row.key());
                        }

                        if (keys.isEmpty()) {
                            log.info("Consistency check finished [grp=" + grpCtx.cacheOrGroupName() +
                                ", part=" + p + ", checked=" + cnt + "]");

                            break;
                        }

                        try {
                            cache.getAll(keys); // Repair.
                        }
                        catch (CacheException e) {
                            if (!(e.getCause() instanceof IgniteConsistencyViolationException) // Found but not fixed.
                                && !isCancelled())
                                throw new IgniteException("Read repair attempt failed.", e);
                        }

                        cnt += keys.size();

                        if (System.currentTimeMillis() >= statusTs) {
                            statusTs = System.currentTimeMillis() + statusDelay;

                            log.info("Consistency check progress [grp=" + grpCtx.cacheOrGroupName() +
                                ", part=" + p + ", checked=" + cnt + "/" + part.fullSize() + "]");

                            VisorConsistencyStatusTask.MAP.put(arg, cnt + "/" + part.fullSize());
                        }

                    }
                    while (!isCancelled());
                }
                finally {
                    ignite.events().stopLocalListen(lsnr);
                }
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Partition repair attempt failed.", e);
            }
            finally {
                part.release();

                VisorConsistencyStatusTask.MAP.remove(arg);
            }

            if (!evts.isEmpty())
                return processEvents(cctx, p, cnt);
            else
                return NOTHING_FOUND + " [processed=" + cnt + "]\n";
        }

        /**
         *
         */
        private String processEvents(GridCacheContext<Object, Object> cctx, int part, long cnt) {
            int found = 0;
            int fixed = 0;

            StringBuilder sb = new StringBuilder();

            for (CacheConsistencyViolationEvent evt : evts) {
                for (Map.Entry<Object, Map<ClusterNode, CacheConsistencyViolationEvent.EntryInfo>> entry : evt.getEntries().entrySet()) {
                    Object key = entry.getKey();

                    if (cctx.affinity().partition(key) != part)
                        continue; // Skipping other partitions results, which are generated by concurrent executions.

                    found++;

                    sb.append("Key: ").append(key)
                        .append(" (Cache: ").append(evt.getCacheName()).append(")").append("\n");

                    for (Map.Entry<ClusterNode, CacheConsistencyViolationEvent.EntryInfo> mapping : entry.getValue().entrySet()) {
                        ClusterNode node = mapping.getKey();
                        CacheConsistencyViolationEvent.EntryInfo info = mapping.getValue();

                        sb.append("  Node: ").append(node).append("\n")
                            .append("    Value: ").append(info.getValue()).append("\n")
                            .append("    Version: ").append(info.getVersion()).append("\n")
                            .append("    Other cluster version: ").append(info.getVersion().otherClusterVersion()).append("\n")
                            .append("    On primary: ").append(info.isPrimary()).append("\n")
                            .append("    Considered as a correct value: ").append(info.isCorrect()).append("\n");

                        if (info.isCorrect())
                            fixed++;
                    }
                }
            }

            String res = sb.toString();

            if (!res.isEmpty()) {
                log.warning(CONSISTENCY_VIOLATIONS_RECORDED + "\n" + res);

                return CONSISTENCY_VIOLATIONS_FOUND + " [found=" + found + ", fixed=" + fixed + ", processed=" + cnt + "]";
            }
            else
                return NOTHING_FOUND + " [processed=" + cnt + "]\n";
        }

        /**
         *
         */
        private class CacheConsistencyViolationEventListener implements IgnitePredicate<CacheConsistencyViolationEvent> {
            /** Serial version uid. */
            private static final long serialVersionUID = 0L;

            /**
             * {@inheritDoc}
             */
            @Override public boolean apply(CacheConsistencyViolationEvent e) {
                assert e instanceof CacheConsistencyViolationEvent;

                evts.add(e);

                return true;
            }
        }
    }
}
