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

package org.apache.ignite.internal.processors.cache.checker.processor;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.checker.objects.ReconciliationAffectedEntries;
import org.apache.ignite.internal.processors.cache.checker.objects.ReconciliationAffectedEntriesExtended;
import org.apache.ignite.internal.processors.cache.checker.objects.VersionedKey;
import org.apache.ignite.internal.processors.cache.checker.objects.VersionedValue;
import org.apache.ignite.internal.processors.cache.checker.util.ConsistencyCheckUtils;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationDataRowMeta;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationKeyMeta;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationRepairMeta;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationSkippedEntityHolder;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationValueMeta;
import org.apache.ignite.internal.processors.cache.verify.RepairMeta;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.U;

import static java.io.File.separatorChar;
import static org.apache.ignite.internal.processors.cache.checker.objects.ReconciliationAffectedEntries.getConflictsAsString;
import static org.apache.ignite.internal.processors.cache.checker.util.ConsistencyCheckUtils.createLocalResultFile;
import static org.apache.ignite.internal.processors.cache.checker.util.ConsistencyCheckUtils.mapPartitionReconciliation;
import static org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationSkippedEntityHolder.SkippingReason.KEY_WAS_NOT_REPAIRED;

/**
 * Defines a contract for collecting of inconsistent and repaired entries.
 */
public interface ReconciliationResultCollector {
    /**
     * Appends skippend entries.
     *
     * @param cacheName Cache name.
     * @param partId Partition id.
     * @param keys Skipped entries
     */
    void appendSkippedEntries(String cacheName, int partId, Map<VersionedKey, Map<UUID, VersionedValue>> keys);

    /**
     * Appends conflicted entries.
     *
     * @param cacheName Cache name.
     * @param partId Partition id.
     * @param conflicts Conflicted entries.
     * @param actualKeys Actual values.
     */
    void appendConflictedEntries(
        String cacheName,
        int partId,
        Map<KeyCacheObject, Map<UUID, GridCacheVersion>> conflicts,
        Map<KeyCacheObject, Map<UUID, VersionedValue>> actualKeys);

    /**
     * Appends repaired entries.
     *
     * @param cacheName Cache name.
     * @param partId Partition Id.
     * @param repairedKeys Repaired entries.
     */
    void appendRepairedEntries(
        String cacheName,
        int partId,
        Map<VersionedKey, RepairMeta> repairedKeys);

    /**
     * This method is called when the given partition is completely processed.
     *
     * @param cacheName Cache name.
     * @param partId Partitin id.
     */
    void onPartitionProcessed(String cacheName, int partId);

    /**
     * Returns partition reconciliation result.
     * @return Partition reconciliation result.
     */
    ReconciliationAffectedEntries result();

    /**
     * Flushes collected result to a file.
     *
     * @param startTime Start time.
     * @return File which contains the result or {@code null} if there are no insistent keys.
     */
    File flushResultsToFile(LocalDateTime startTime);

    /**
     * Represents a collector of inconsistent and repaired entries.
     */
    public static class Simple implements ReconciliationResultCollector {
        /** Ignite instance. */
        protected final IgniteEx ignite;

        /** Logger. */
        protected final IgniteLogger log;

        /** Indicates that sensitive information should be stored. */
        protected final boolean includeSensitive;

        /** Root folder. */
        protected final File reconciliationDir;

        /** Keys that were detected as incosistent during the reconciliation process. */
        protected final Map<String, Map<Integer, List<PartitionReconciliationDataRowMeta>>> inconsistentKeys = new HashMap<>();

        /** Entries that were detected as inconsistent but weren't repaired due to some reason. */
        protected final Map<String, Map<Integer, Set<PartitionReconciliationSkippedEntityHolder<PartitionReconciliationKeyMeta>>>> skippedEntries = new HashMap<>();

        /**
         * Creates a new SimpleCollector.
         *
         * @param ignite Ignite instance.
         * @param log Ignite logger.
         * @param includeSensitive {@code true} if sensitive information should be preserved.
         * @throws IgniteCheckedException If reconciliation parent directory cannot be created/resolved.
         */
        public Simple(
            IgniteEx ignite,
            IgniteLogger log,
            boolean includeSensitive
        ) throws IgniteCheckedException {
            this.ignite = ignite;
            this.log = log;
            this.includeSensitive = includeSensitive;

            synchronized (ReconciliationResultCollector.class) {
                reconciliationDir = new File(U.defaultWorkDirectory() + separatorChar + ConsistencyCheckUtils.RECONCILIATION_DIR);

                if (!reconciliationDir.exists())
                    reconciliationDir.mkdir();
            }
        }

        /** {@inheritDoc} */
        @Override public void appendSkippedEntries(
            String cacheName,
            int partId,
            Map<VersionedKey, Map<UUID, VersionedValue>> keys
        ) {
            CacheObjectContext ctx = ignite.cachex(cacheName).context().cacheObjectContext();

            synchronized (skippedEntries) {
                Set<PartitionReconciliationSkippedEntityHolder<PartitionReconciliationKeyMeta>> data = new HashSet<>();

                for (VersionedKey keyVersion : keys.keySet()) {
                    try {
                        byte[] bytes = keyVersion.key().valueBytes(ctx);
                        String strVal = ConsistencyCheckUtils.objectStringView(ctx, keyVersion.key().value(ctx, false));

                        PartitionReconciliationSkippedEntityHolder<PartitionReconciliationKeyMeta> holder
                            = new PartitionReconciliationSkippedEntityHolder<>(
                            new PartitionReconciliationKeyMeta(bytes, strVal),
                            KEY_WAS_NOT_REPAIRED
                        );

                        data.add(holder);
                    }
                    catch (Exception e) {
                        log.error("Serialization problem.", e);
                    }
                }

                skippedEntries
                    .computeIfAbsent(cacheName, k -> new HashMap<>())
                    .computeIfAbsent(partId, l -> new HashSet<>())
                    .addAll(data);
            }
        }

        /** {@inheritDoc} */
        @Override public void appendConflictedEntries(
            String cacheName,
            int partId,
            Map<KeyCacheObject, Map<UUID, GridCacheVersion>> conflicts,
            Map<KeyCacheObject, Map<UUID, VersionedValue>> actualKeys
        ) {
            CacheObjectContext ctx = ignite.cachex(cacheName).context().cacheObjectContext();

            synchronized (inconsistentKeys) {
                try {
                    inconsistentKeys.computeIfAbsent(cacheName, k -> new HashMap<>())
                        .computeIfAbsent(partId, k -> new ArrayList<>())
                        .addAll(mapPartitionReconciliation(conflicts, actualKeys, ctx));
                }
                catch (IgniteCheckedException e) {
                    log.error("Broken key can't be added to result. ", e);
                }
            }
        }

        /** {@inheritDoc} */
        @Override public void appendRepairedEntries(
            String cacheName,
            int partId,
            Map<VersionedKey, RepairMeta> repairedKeys
        ) {
            CacheObjectContext ctx = ignite.cachex(cacheName).context().cacheObjectContext();

            synchronized (inconsistentKeys) {
                try {
                    List<PartitionReconciliationDataRowMeta> res = new ArrayList<>();

                    for (Map.Entry<VersionedKey, RepairMeta> entry : repairedKeys.entrySet()) {
                        Map<UUID, PartitionReconciliationValueMeta> valMap = new HashMap<>();

                        for (Map.Entry<UUID, VersionedValue> uuidBasedEntry : entry.getValue().getPreviousValue().entrySet()) {
                            Optional<CacheObject> cacheObjOpt = Optional.ofNullable(uuidBasedEntry.getValue().value());

                            valMap.put(
                                uuidBasedEntry.getKey(),
                                cacheObjOpt.isPresent() ?
                                    new PartitionReconciliationValueMeta(
                                        cacheObjOpt.get().valueBytes(ctx),
                                        cacheObjOpt.map(o -> ConsistencyCheckUtils.objectStringView(ctx, o)).orElse(null),
                                        uuidBasedEntry.getValue().version())
                                    :
                                    null);
                        }

                        KeyCacheObject key = entry.getKey().key();

                        key.finishUnmarshal(ctx, null);

                        RepairMeta repairMeta = entry.getValue();

                        Optional<CacheObject> cacheObjRepairValOpt = Optional.ofNullable(repairMeta.value());

                        res.add(
                            new PartitionReconciliationDataRowMeta(
                                new PartitionReconciliationKeyMeta(
                                    key.valueBytes(ctx),
                                    ConsistencyCheckUtils.objectStringView(ctx, key)),
                                valMap,
                                new PartitionReconciliationRepairMeta(
                                    repairMeta.fixed(),
                                    cacheObjRepairValOpt.isPresent() ?
                                        new PartitionReconciliationValueMeta(
                                            cacheObjRepairValOpt.get().valueBytes(ctx),
                                            cacheObjRepairValOpt.map(o -> ConsistencyCheckUtils.objectStringView(ctx, o)).orElse(null),
                                            null)
                                        :
                                        null,
                                    repairMeta.repairAlg())));
                    }

                    inconsistentKeys.computeIfAbsent(cacheName, k -> new HashMap<>())
                        .computeIfAbsent(partId, k -> new ArrayList<>())
                        .addAll(res);
                }
                catch (IgniteCheckedException e) {
                    log.error("Broken key can't be added to result. ", e);
                }
            }
        }

        /** {@inheritDoc} */
        @Override public void onPartitionProcessed(String cacheName, int partId) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public ReconciliationAffectedEntries result() {
            synchronized (inconsistentKeys) {
                synchronized (skippedEntries) {
                    return new ReconciliationAffectedEntries(
                        ignite.cluster().nodes().stream().collect(Collectors.toMap(
                            ClusterNode::id,
                            n -> n.consistentId().toString())),
                        inconsistentKeys,
                        skippedEntries
                    );
                }
            }
        }

        /** {@inheritDoc} */
        @Override public File flushResultsToFile(LocalDateTime startTime) {
            ReconciliationAffectedEntries res = result();

            if (res != null && !res.isEmpty()) {
                try {
                    File file = createLocalResultFile(ignite.context().discovery().localNode(), startTime);

                    try (PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(file)))) {
                        res.print(pw::write, includeSensitive);

                        pw.flush();

                        return file;
                    }
                    catch (IOException e) {
                        log.error("Unable to write report to file " + e.getMessage());
                    }
                }
                catch (IgniteCheckedException | IOException e) {
                    log.error("Unable to create file " + e.getMessage());
                }
            }

            return null;
        }
    }

    /**
     * Represents a collector of inconsistent and repaired entries that persists temporary results (per partition).
     */
    public static class Compact extends Simple {
        /** Total number of inconsistent keys. */
        private final AtomicInteger totalKeysCnt = new AtomicInteger();

        /** Provides mapping of cache name to temporary filename with results. */
        private final Map<String, String> tmpFiles = new ConcurrentHashMap<>();

        /** Mapping of node Ids to consistent Ids. */
        private final Map<UUID, String> nodesIdsToConsistentIdsMap =  ignite.cluster().nodes().stream()
            .collect(Collectors.toMap(ClusterNode::id, n -> n.consistentId().toString()));

        /** Session identifier. */
        private final long sesId;

        /**
         * Creates a new collector.
         *
         * @param ignite Ignite instance.
         * @param log Ignite logger.
         * @param includeSensitive {@code true} if sensitive information should be preserved.
         * @throws IgniteCheckedException If reconciliation parent directory cannot be created/resolved.
         */
        Compact(
            IgniteEx ignite,
            IgniteLogger log,
            long sesId,
            boolean includeSensitive
        ) throws IgniteCheckedException {
            super(ignite, log, includeSensitive);

            this.sesId = sesId;
        }

        /** {@inheritDoc} */
        @Override public void onPartitionProcessed(String cacheName, int partId) {
            if (log.isDebugEnabled())
                log.debug("Partition has been processed [cacheName=" + cacheName + ", partId=" + partId + ']');

            List<PartitionReconciliationDataRowMeta> meta = null;

            synchronized (inconsistentKeys) {
                Map<Integer, List<PartitionReconciliationDataRowMeta>> c = inconsistentKeys.get(cacheName);
                if (c != null)
                     meta = c.remove(partId);
            }

            if (meta != null && !meta.isEmpty()) {
                totalKeysCnt.addAndGet(meta.size());

                storePartition(cacheName, partId, meta);
            }
        }

        /** {@inheritDoc} */
        @Override public ReconciliationAffectedEntries result() {
            synchronized (inconsistentKeys) {
                synchronized (skippedEntries) {
                    return new ReconciliationAffectedEntriesExtended(
                        totalKeysCnt.get(),
                        0, // skipped caches count which is not tracked/used.
                        skippedEntries.size());
                }
            }
        }

        /** {@inheritDoc} */
        @Override public File flushResultsToFile(LocalDateTime startTime) {
            if (!tmpFiles.isEmpty()) {
                try {
                    File file = createLocalResultFile(ignite.context().discovery().localNode(), startTime);

                    try (PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(file)))) {
                        printFileHead(out, totalKeysCnt.get());

                        for (Map.Entry<String, String> e : tmpFiles.entrySet()) {
                            out.println(e.getKey());

                            try (BufferedReader reader = new BufferedReader(new FileReader(e.getValue()))) {
                                String line;
                                while ((line = reader.readLine()) != null)
                                    out.println(line);
                            }

                            Files.delete(Paths.get(e.getValue()));
                        }

                        out.flush();

                        return file;
                    }
                    catch (IOException e) {
                        log.error("Unable to write report to file " + e.getMessage());
                    }
                }
                catch (IgniteCheckedException | IOException e) {
                    log.error("Unable to create file " + e.getMessage());
                }
            }

            return null;
        }

        /**
         * Prints common information.
         *
         * @param out Output stream.
         * @param inconsistentKeyCnt Number of inconsistent keys.
         */
        private void printFileHead(PrintWriter out, long inconsistentKeyCnt) {
            out.println();
            out.println("INCONSISTENT KEYS: " + inconsistentKeyCnt);
            out.println();

            out.println("<cacheName>");
            out.println("\t<partitionId>");
            out.println("\t\t<key>");
            out.println("\t\t\t<nodeConsistentId>, <nodeId>: <value> <version>");
            out.println("\t\t\t...");
            out.println("\t\t\t<info on whether confilct is fixed>");
            out.println();
        }

        /**
         * Stores the result of partition reconciliation for the given partition.
         *
         * @param cacheName Cache name.
         * @param partId Partition id.
         * @param meta Entries to store.
         */
        private void storePartition(String cacheName, int partId, List<PartitionReconciliationDataRowMeta> meta) {
            String maskId = U.maskForFileName(ignite.context().discovery().localNode().consistentId().toString());

            String fileName = tmpFiles.computeIfAbsent(cacheName, d -> {
                File file = new File(reconciliationDir.getPath() + separatorChar + maskId + '-' + sesId + '-' + cacheName + ".txt");
                try {
                    file.createNewFile();
                }
                catch (IOException e) {
                    log.error("Cannot create a file for storing partition's data " +
                        "[cacheName=" + cacheName + ", partId" + partId + ']');

                    return null;
                }

                return file.getAbsolutePath();
            });

            // Cannot create temporary file. Error message is already logged.
            if (fileName == null)
                return;

            synchronized (fileName) {
                try (PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(fileName, true)))) {
                    out.print('\t');
                    out.println(partId);

                    for (PartitionReconciliationDataRowMeta row : meta)
                        out.print(getConflictsAsString(row, nodesIdsToConsistentIdsMap, true));
                }
                catch (IOException e) {
                    log.error("Cannot store partition's data [cacheName=" + cacheName + ", partId" + partId + ']');
                }
            }
        }
    }
}
