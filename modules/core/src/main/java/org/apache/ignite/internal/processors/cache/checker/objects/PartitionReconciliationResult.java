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

package org.apache.ignite.internal.processors.cache.checker.objects;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationDataRowMeta;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationKeyMeta;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationSkippedEntityHolder;
import org.apache.ignite.internal.processors.cache.verify.PartitionReconciliationValueMeta;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Result with lists of broken and fixed, skipped entries.
 */
public class PartitionReconciliationResult extends IgniteDataTransferObject {
    /**
     *
     */
    private static final long serialVersionUID = 0L;

    /** A sequence of characters that is used to hide sensitive data in case of non-verbose mode. */
    public static final String HIDDEN_DATA = "*****";

    /** Map of node ids to node consistent ids. */
    private Map<UUID, String> nodesIdsToConsistenceIdsMap = new HashMap<>();

    /** Inconsistent keys. */
    private Map<String, Map<Integer, List<PartitionReconciliationDataRowMeta>>> inconsistentKeys = new HashMap<>();

    /** Skipped caches. */
    private Set<PartitionReconciliationSkippedEntityHolder<String>> skippedCaches = new HashSet<>();

    /** Skipped entries. */
    private Map<String, Map<Integer, Set<PartitionReconciliationSkippedEntityHolder<PartitionReconciliationKeyMeta>>>>
        skippedEntries = new HashMap<>();

    /**
     * Default constructor for externalization.
     */
    public PartitionReconciliationResult() {
    }

    /**
     * @param nodesIdsToConsistenceIdsMap Nodes ids to consistence ids map.
     * @param inconsistentKeys Inconsistent keys.
     * @param skippedEntries Skipped entries.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public PartitionReconciliationResult(
        Map<UUID, String> nodesIdsToConsistenceIdsMap,
        Map<String, Map<Integer, List<PartitionReconciliationDataRowMeta>>> inconsistentKeys,
        Map<String, Map<Integer, Set<PartitionReconciliationSkippedEntityHolder<PartitionReconciliationKeyMeta>>>>
            skippedEntries) {
        this.nodesIdsToConsistenceIdsMap = nodesIdsToConsistenceIdsMap;
        this.inconsistentKeys = inconsistentKeys;
        this.skippedEntries = skippedEntries;
    }

    /**
     * @param nodesIdsToConsistenceIdsMap Nodes ids to consistence ids map.
     * @param inconsistentKeys Inconsistent keys.
     * @param skippedCaches Skipped caches.
     * @param skippedEntries Skipped entries.
     */
    public PartitionReconciliationResult(
        Map<UUID, String> nodesIdsToConsistenceIdsMap,
        Map<String, Map<Integer, List<PartitionReconciliationDataRowMeta>>> inconsistentKeys,
        Set<PartitionReconciliationSkippedEntityHolder<String>> skippedCaches,
        Map<String, Map<Integer, Set<PartitionReconciliationSkippedEntityHolder<PartitionReconciliationKeyMeta>>>>
            skippedEntries) {
        this.nodesIdsToConsistenceIdsMap = nodesIdsToConsistenceIdsMap;
        this.inconsistentKeys = inconsistentKeys;
        this.skippedCaches = skippedCaches;
        this.skippedEntries = skippedEntries;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeMap(out, nodesIdsToConsistenceIdsMap);

        U.writeMap(out, inconsistentKeys);

        U.writeCollection(out, skippedCaches);

        U.writeMap(out, skippedEntries);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in)
        throws IOException, ClassNotFoundException {
        nodesIdsToConsistenceIdsMap = U.readMap(in);

        inconsistentKeys = U.readMap(in);

        skippedCaches = U.readSet(in);

        skippedEntries = U.readMap(in);
    }

    /**
     * Fills printer {@code Consumer<String>} by string view of this class.
     */
    public void print(Consumer<String> printer, boolean verbose) {
        if (inconsistentKeys != null && !inconsistentKeys.isEmpty()) {
            printer.accept("\nINCONSISTENT KEYS: " + inconsistentKeysCount() + "\n\n");

            printer.accept("<cacheName>\n");
            printer.accept("\t<partitionId>\n");
            printer.accept("\t\t<key>\n");
            printer.accept("\t\t\t<nodeConsistentId>, <nodeId>: <value> <version>\n");
            printer.accept("\t\t\t...\n");
            printer.accept("\t\t\t<info on whether confilct is fixed>\n\n");

            for (Map.Entry<String, Map<Integer, List<PartitionReconciliationDataRowMeta>>>
                cacheBoundedInconsistentKeysEntry : inconsistentKeys.entrySet()) {

                String cacheName = cacheBoundedInconsistentKeysEntry.getKey();

                printer.accept(cacheName + "\n");

                for (Map.Entry<Integer, List<PartitionReconciliationDataRowMeta>> partitionBoundedInconsistentKeysEntry
                    : cacheBoundedInconsistentKeysEntry.getValue().entrySet()) {
                    Integer part = partitionBoundedInconsistentKeysEntry.getKey();

                    printer.accept("\t" + part + "\n");

                    for (PartitionReconciliationDataRowMeta inconsistentDataRow :
                        partitionBoundedInconsistentKeysEntry.getValue()) {
                        printer.accept("\t\t" + inconsistentDataRow.keyMeta().stringView(verbose) + "\n");

                        for (Map.Entry<UUID, PartitionReconciliationValueMeta> valMap :
                            inconsistentDataRow.valueMeta().entrySet()) {
                            printer.accept("\t\t\t" + nodesIdsToConsistenceIdsMap.get(valMap.getKey()) + " " +
                                U.id8(valMap.getKey()) +
                                (valMap.getValue() != null ? ": " + valMap.getValue().stringView(verbose) : "") + "\n");
                        }

                        if (inconsistentDataRow.repairMeta() != null) {
                            printer.accept("\n\t\t\t" +
                                inconsistentDataRow.repairMeta().stringView(verbose) + "\n\n");
                        }
                    }
                }
            }
        }

        if (skippedCaches != null && !skippedCaches.isEmpty()) {
            printer.accept("\nSKIPPED CACHES: " + skippedCachesCount() + "\n\n");

            for (PartitionReconciliationSkippedEntityHolder<String> skippedCache : skippedCaches) {
                printer.accept("Following cache was skipped during partition reconciliation check cache=["
                    + skippedCache.skippedEntity() + "], reason=[" + skippedCache.skippingReason() + "]\n");
            }
        }

        if (skippedEntries != null && !skippedEntries.isEmpty()) {
            printer.accept("\nSKIPPED ENTRIES: " + skippedEntriesCount() + "\n\n");

            for (Map.Entry<String, Map<Integer, Set<PartitionReconciliationSkippedEntityHolder<PartitionReconciliationKeyMeta>>>>
                cacheBoundedSkippedEntries : skippedEntries.entrySet()) {
                String cacheName = cacheBoundedSkippedEntries.getKey();

                for (Map.Entry<Integer, Set<PartitionReconciliationSkippedEntityHolder<PartitionReconciliationKeyMeta>>>
                    partitionBoundedSkippedEntries
                    : cacheBoundedSkippedEntries.getValue().entrySet()) {
                    StringBuilder recordBuilder = new StringBuilder();

                    Integer part = partitionBoundedSkippedEntries.getKey();

                    recordBuilder.append("Following entry was skipped [cache='").append(cacheName).append("'");

                    recordBuilder.append(", partition=").append(part);

                    for (PartitionReconciliationSkippedEntityHolder<PartitionReconciliationKeyMeta> skippedEntry
                        : partitionBoundedSkippedEntries.getValue()) {

                        recordBuilder.append(", entry=").append(skippedEntry.skippedEntity());

                        recordBuilder.append(", reason=").append(skippedEntry.skippingReason());
                    }
                    recordBuilder.append("]\n");

                    printer.accept(recordBuilder.toString());
                }
            }
        }
    }

    /**
     * Added outer value to this class.
     */
    public void merge(PartitionReconciliationResult outer) {
        assert outer instanceof PartitionReconciliationResult;

        this.nodesIdsToConsistenceIdsMap.putAll(outer.nodesIdsToConsistenceIdsMap);

        for (Map.Entry<String, Map<Integer, List<PartitionReconciliationDataRowMeta>>> entry : outer.inconsistentKeys.entrySet()) {
            Map<Integer, List<PartitionReconciliationDataRowMeta>> map = this.inconsistentKeys.computeIfAbsent(entry.getKey(), key -> new HashMap<>());

            for (Map.Entry<Integer, List<PartitionReconciliationDataRowMeta>> listEntry : entry.getValue().entrySet())
                map.computeIfAbsent(listEntry.getKey(), k -> new ArrayList<>()).addAll(listEntry.getValue());
        }

        for (Map.Entry<String, Map<Integer, Set<PartitionReconciliationSkippedEntityHolder<PartitionReconciliationKeyMeta>>>> entry : outer.skippedEntries.entrySet()) {
            Map<Integer, Set<PartitionReconciliationSkippedEntityHolder<PartitionReconciliationKeyMeta>>> map = this.skippedEntries.computeIfAbsent(entry.getKey(), key -> new HashMap<>());

            for (Map.Entry<Integer, Set<PartitionReconciliationSkippedEntityHolder<PartitionReconciliationKeyMeta>>> setEntry : entry.getValue().entrySet())
                map.computeIfAbsent(setEntry.getKey(), k -> new HashSet<>()).addAll(setEntry.getValue());
        }

        this.skippedCaches.addAll(outer.skippedCaches);
    }

    /**
     * @return {@code True} if reconciliation result doesn't contain neither inconsistent keys, nor skipped caches, etc.
     */
    public boolean isEmpty() {
        return inconsistentKeys.isEmpty() && skippedCaches.isEmpty() && skippedEntries().isEmpty();
    }

    /**
     * Mapping node ids to consistence ids.
     */
    public Map<UUID, String> nodesIdsToConsistenceIdsMap() {
        return nodesIdsToConsistenceIdsMap;
    }

    /**
     * Broken keys.
     */
    public Map<String, Map<Integer, List<PartitionReconciliationDataRowMeta>>> inconsistentKeys() {
        return inconsistentKeys;
    }

    /**
     * Skipped caches.
     */
    public Set<PartitionReconciliationSkippedEntityHolder<String>> skippedCaches() {
        return skippedCaches;
    }

    /**
     * Skipped entries.
     */
    public Map<String, Map<Integer, Set<PartitionReconciliationSkippedEntityHolder<PartitionReconciliationKeyMeta>>>> skippedEntries() {
        return skippedEntries;
    }

    /**
     * @return Inconsisitent keys count.
     */
    public int inconsistentKeysCount() {
        return inconsistentKeys.values().stream().flatMap(m -> m.values().stream()).mapToInt(List::size).sum();
    }

    /**
     * @return Skipped caches count.
     */
    public int skippedCachesCount() {
        return skippedCaches.size();
    }

    /**
     * @return Skipped entries count.
     */
    public int skippedEntriesCount() {
        return skippedEntries.size();
    }
}
