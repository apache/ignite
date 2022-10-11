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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.ReadRepairStrategy;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.CacheConsistencyViolationEvent;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.near.consistency.IgniteIrreparableConsistencyViolationException;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
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
            ExecutorService sys = ignite.context().pools().getSystemExecutorService();

            StringBuilder res = new StringBuilder();

            arg.parts().stream()
                .map(p -> F.t(p, sys.submit(() -> processPartition(p, arg))))
                .map(t -> {
                    try {
                        return F.t(t.get1(), t.get2().get());
                    }
                    catch (ExecutionException | InterruptedException e) {
                        throw new IgniteException("Consistency task was interrupted", e);
                    }
                })
                .filter(t -> t.get2() != null)
                .forEach(t -> res.append("    Partition ").append(t.get1()).append(' ').append(t.get2()).append('\n'));

            return res.toString();
        }

        /**
         * @param p Partition.
         * @param arg Taks arguments.
         * @return Partition results.
         */
        private String processPartition(int p, VisorConsistencyRepairTaskArg arg) {
            String cacheOrGrpName = arg.cacheOrGroupName();
            ReadRepairStrategy strategy = arg.strategy();

            int batchSize = 128;
            int statusDelay = 60_000; // Every minute.

            int cacheOrGrpId = CU.cacheId(cacheOrGrpName);

            CacheGroupContext grpCtx = ignite.context().cache().cacheGroup(cacheOrGrpId);

            if (grpCtx == null) {
                GridCacheContext<?, ?> cacheCtx = ignite.context().cache().context().cacheContext(cacheOrGrpId);

                if (cacheCtx != null)
                    grpCtx = cacheCtx.group();
            }

            if (grpCtx == null)
                if (ignite.context().cache().cacheGroupDescriptor(cacheOrGrpId) != null ||
                    ignite.context().cache().cacheDescriptor(cacheOrGrpName) != null)
                    return null;
                else
                    throw new IgniteException("Cache (or cache group) not found [name=" + cacheOrGrpName + "]");

            if (!ignite.context().event().isRecordable(EVT_CONSISTENCY_VIOLATION))
                throw new UnsupportedOperationException("Consistency violation events recording is disabled on cluster.");

            GridDhtLocalPartition part = grpCtx.topology().localPartition(p);

            if (part == null)
                return null;

            log.info("Consistency check started " +
                "[grp=" + grpCtx.cacheOrGroupName() + ", part=" + p + ", strategy=" + strategy + "]");

            String statusKey = "[node=" + ignite.localNode() + ", cacheGroup=" + grpCtx.cacheOrGroupName() + ", part=" + p + "]";

            if (VisorConsistencyStatusTask.MAP.putIfAbsent(statusKey, "0/" + part.fullSize()) != null) {
                throw new IllegalStateException("Consistency check already started " +
                    "[grp=" + grpCtx.cacheOrGroupName() + ", part=" + p + "]");
            }

            long processed = 0;
            long checked = 0;
            long statusTs = 0;

            part.reserve();

            try {
                IgnitePredicate<CacheConsistencyViolationEvent> lsnr = new CacheConsistencyViolationEventListener(
                    grpCtx.caches().stream().map(GridCacheContext::name).collect(Collectors.toSet()));

                ignite.events().localListen(lsnr, EVT_CONSISTENCY_VIOLATION);

                try {
                    Map<Integer, PerCacheBatch> batches = new HashMap<>();

                    GridCursor<? extends CacheDataRow> cursor = grpCtx.offheap().dataStore(part).cursor();

                    while (cursor.next() && !isCancelled()) {
                        CacheDataRow row = cursor.get();

                        processed++;

                        PerCacheBatch batch = batches.computeIfAbsent(row.cacheId(), cacheId -> {
                            String cacheName = cacheId != 0 ?
                                ignite.context().cache().cacheDescriptor(cacheId).cacheName() :
                                cacheOrGrpName;

                            return new PerCacheBatch(ignite.cache(cacheName).withKeepBinary().withReadRepair(strategy));
                        });

                        batch.keys.add(row.key());

                        if (batch.keys.size() == batchSize) {
                            repair(batch.cache, batch.keys);

                            checked += batch.keys.size();

                            batch.keys.clear();

                            VisorConsistencyStatusTask.MAP.put(statusKey, checked + "/" + part.fullSize());
                        }

                        assert batch.keys.size() < batchSize;

                        if (System.currentTimeMillis() >= statusTs) {
                            statusTs = System.currentTimeMillis() + statusDelay;

                            log.info("Consistency check progress [grp=" + grpCtx.cacheOrGroupName() +
                                ", caches=" + batches.values().stream().map(b -> b.cache.getName()).collect(Collectors.toList()) +
                                ", part=" + p +
                                ", checked=" + checked +
                                ", processed =" + processed + "/" + part.fullSize() + "]");
                        }
                    }

                    for (PerCacheBatch batch : batches.values()) {
                        assert batch.keys.size() < batchSize;

                        repair(batch.cache, batch.keys);

                        checked += batch.keys.size();

                        batch.keys.clear();
                    }

                    log.info("Consistency check " + (isCancelled() ? "cancelled" : "finished") +
                        "[grp=" + grpCtx.cacheOrGroupName() +
                        ", caches=" + batches.values().stream().map(b -> b.cache.getName()).collect(Collectors.toList()) +
                        ", part=" + p +
                        ", checked=" + checked +
                        ", processed =" + processed + "/" + part.fullSize() + "]");
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

                VisorConsistencyStatusTask.MAP.remove(statusKey);
            }

            if (!evts.isEmpty())
                return processEvents(p, checked);
            else
                return NOTHING_FOUND + " [processed=" + checked + "]\n";
        }

        /**
         * @param cache Cache.
         * @param keys Keys.
         */
        private void repair(IgniteCache<Object, Object> cache, Set<Object> keys) {
            try {
                cache.getAll(keys); // Repair.
            }
            catch (CacheException e) {
                if (!(e.getCause() instanceof IgniteIrreparableConsistencyViolationException) // Found but not repaired.
                    && !isCancelled())
                    throw new IgniteException("Read repair attempt failed.", e);
            }
        }

        /**
         *
         */
        private String processEvents(int part, long cnt) {
            int found = 0;
            int repaired = 0;

            StringBuilder sb = new StringBuilder();

            for (CacheConsistencyViolationEvent evt : evts) {
                for (Map.Entry<?, CacheConsistencyViolationEvent.EntriesInfo> entry : evt.getEntries().entrySet()) {
                    Object key = entry.getKey();

                    if (entry.getValue().partition() != part)
                        continue; // Skipping other partitions results, which are generated by concurrent executions.

                    found++;

                    sb.append("Key: ").append(key)
                        .append(" (cache: ").append(evt.getCacheName())
                        .append(", partition: ").append(entry.getValue().partition())
                        .append(", strategy: ").append(evt.getStrategy())
                        .append(", id: ").append(evt.id())
                        .append(", timestamp: ").append(evt.timestamp())
                        .append(", node: ").append(evt.node())
                        .append(")").append("\n");

                    if (evt.getRepairedEntries().containsKey(key))
                        sb.append(" Repaired: ").append(evt.getRepairedEntries().get(key)).append("\n");
                    else
                        sb.append(" [Was NOT repaired!]").append("\n");

                    for (Map.Entry<ClusterNode, CacheConsistencyViolationEvent.EntryInfo> mapping :
                        entry.getValue().getMapping().entrySet()) {
                        ClusterNode node = mapping.getKey();
                        CacheConsistencyViolationEvent.EntryInfo info = mapping.getValue();

                        sb.append("  Node: ").append(node).append("\n")
                            .append("    Value: ").append(info.getValue()).append("\n")
                            .append("    Version: ").append(info.getVersion()).append("\n")
                            .append("    On primary: ").append(info.isPrimary()).append("\n");

                        if (info.getVersion() != null)
                            sb.append("    Other cluster version: ").append(info.getVersion().otherClusterVersion()).append("\n");

                        if (info.isCorrect())
                            sb.append("    [CORRECT value!]").append("\n");
                    }

                    if (evt.getRepairedEntries().containsKey(key))
                        repaired++;
                }
            }

            String res = sb.toString();

            if (!res.isEmpty()) {
                log.warning(CONSISTENCY_VIOLATIONS_RECORDED + "\n" + res);

                return CONSISTENCY_VIOLATIONS_FOUND + " [found=" + found + ", repaired=" + repaired + ", processed=" + cnt + "]";
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

            /** Cache names. */
            private final Set<String> cacheNames;

            /**
             * @param cacheNames Names.
             */
            private CacheConsistencyViolationEventListener(Set<String> cacheNames) {
                this.cacheNames = cacheNames;
            }

            /**
             * {@inheritDoc}
             */
            @Override public boolean apply(CacheConsistencyViolationEvent evt) {
                assert evt instanceof CacheConsistencyViolationEvent;

                if (!cacheNames.contains(evt.getCacheName()))
                    return true; // Skipping other caches results, which are generated by concurrent executions.

                evts.add(evt);

                return true;
            }
        }

        /**
         *
         */
        private static class PerCacheBatch {
            /** Cache. */
            private final IgniteCache<Object, Object> cache;

            /** Keys. */
            private final Set<Object> keys;

            /**
             * @param cache Cache.
             */
            public PerCacheBatch(IgniteCache<Object, Object> cache) {
                this.cache = cache;
                keys = new HashSet<>();
            }
        }
    }
}
