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
package org.apache.ignite.internal.management.cache;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.cache.verify.PartitionHashRecord;
import org.apache.ignite.internal.processors.cache.verify.TransactionsHashRecord;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.util.IgniteUtils.nl;

/**
 * Encapsulates result of {@link VerifyBackupPartitionsTask}.
 */
public class IdleVerifyResult extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Counter conflicts. */
    @GridToStringInclude
    private Map<PartitionKey, List<PartitionHashRecord>> cntrConflicts;

    /** Hash conflicts. */
    @GridToStringInclude
    private Map<PartitionKey, List<PartitionHashRecord>> hashConflicts;

    /** Moving partitions. */
    @GridToStringInclude
    private Map<PartitionKey, List<PartitionHashRecord>> movingPartitions;

    /** Lost partitions. */
    @GridToStringInclude
    private Map<PartitionKey, List<PartitionHashRecord>> lostPartitions;

    /** Transaction hashes conflicts. */
    @GridToStringInclude
    private List<List<TransactionsHashRecord>> txHashConflicts;

    /** Partial committed transactions. */
    @GridToStringInclude
    private Map<ClusterNode, Collection<GridCacheVersion>> partiallyCommittedTxs;

    /** Exceptions. */
    @GridToStringInclude
    private Map<ClusterNode, Exception> exceptions;

    /**
     * Default public constructor. Mostly for {@link Externalizable}.
     *
     * @see #builder()
     * @see #ofErrors(Map)
     */
    public IdleVerifyResult() {
        this(null, null, null, null, null, null, null);
    }

    /**
     * @see #builder()
     */
    private IdleVerifyResult(
        @Nullable Map<PartitionKey, List<PartitionHashRecord>> cntrConflicts,
        @Nullable Map<PartitionKey, List<PartitionHashRecord>> hashConflicts,
        @Nullable Map<PartitionKey, List<PartitionHashRecord>> movingPartitions,
        @Nullable Map<PartitionKey, List<PartitionHashRecord>> lostPartitions,
        @Nullable List<List<TransactionsHashRecord>> txHashConflicts,
        @Nullable Map<ClusterNode, Collection<GridCacheVersion>> partiallyCommittedTxs,
        @Nullable Map<ClusterNode, Exception> exceptions
    ) {
        this.cntrConflicts = notNullFinalMap(cntrConflicts);
        this.hashConflicts = notNullFinalMap(hashConflicts);
        this.movingPartitions = notNullFinalMap(movingPartitions);
        this.lostPartitions = notNullFinalMap(lostPartitions);
        this.txHashConflicts = notNullFinalList(txHashConflicts);
        this.partiallyCommittedTxs = notNullFinalMap(partiallyCommittedTxs);

        this.exceptions = notNullFinalMap(exceptions);
    }

    /** {@inheritDoc} */
    @Override public byte getProtocolVersion() {
        return V4;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeMap(out, cntrConflicts);
        U.writeMap(out, hashConflicts);
        U.writeMap(out, movingPartitions);
        U.writeMap(out, exceptions);
        U.writeMap(out, lostPartitions);
        U.writeCollection(out, txHashConflicts);
        U.writeMap(out, partiallyCommittedTxs);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        cntrConflicts = notNullFinalMap(U.readMap(in));
        hashConflicts = notNullFinalMap(U.readMap(in));
        movingPartitions = notNullFinalMap(U.readMap(in));

        if (protoVer >= V2)
            exceptions = notNullFinalMap(U.readMap(in));

        if (protoVer >= V3)
            lostPartitions = notNullFinalMap(U.readMap(in));

        if (protoVer >= V4) {
            txHashConflicts = notNullFinalList((List)U.readCollection(in));
            partiallyCommittedTxs = notNullFinalMap(U.readMap(in));
        }
    }

    /**
     * @return Counter conflicts.
     */
    public Map<PartitionKey, List<PartitionHashRecord>> counterConflicts() {
        return cntrConflicts;
    }

    /**
     * @return Hash conflicts.
     */
    public Map<PartitionKey, List<PartitionHashRecord>> hashConflicts() {
        return hashConflicts;
    }

    /**
     * @return Moving partitions.
     */
    public Map<PartitionKey, List<PartitionHashRecord>> movingPartitions() {
        return movingPartitions;
    }

    /**
     * @return Lost partitions.
     */
    public Map<PartitionKey, List<PartitionHashRecord>> lostPartitions() {
        return lostPartitions;
    }

    /**
     * @return {@code true} if any conflicts were discovered during the check.
     */
    public boolean hasConflicts() {
        return !hashConflicts.isEmpty() || !counterConflicts().isEmpty()
            || !txHashConflicts.isEmpty() || !partiallyCommittedTxs.isEmpty();
    }

    /**
     * @return Exceptions on nodes.
     */
    public Map<ClusterNode, Exception> exceptions() {
        return exceptions;
    }

    /**
     * Print formatted result to the given printer.
     *
     * @param printer Consumer for handle formatted result.
     * @param printExceptionMessages {@code true} if exceptions must be included too.
     */
    public void print(Consumer<String> printer, boolean printExceptionMessages) {
        if (exceptions.isEmpty()) {
            if (!hasConflicts())
                printer.accept("The check procedure has finished, no conflicts have been found.\n");
            else
                printConflicts(printer);

            if (!movingPartitions.isEmpty())
                printer.accept("Possible results are not full due to rebalance still in progress." + nl());

            printSkippedPartitions(printer, movingPartitions, "MOVING");
            printSkippedPartitions(printer, lostPartitions, "LOST");

            return;
        }

        int size = exceptions.size();

        printer.accept("The check procedure failed on " + size + " node" + (size == 1 ? "" : "s") + ".\n");

        if (!F.view(exceptions.values(), e -> e instanceof NoMatchingCachesException).isEmpty())
            printer.accept("\nThere are no caches matching given filter options.\n");

        printer.accept("\nThe check procedure failed on nodes:\n");

        for (Map.Entry<ClusterNode, Exception> e : exceptions().entrySet()) {
            ClusterNode n = e.getKey();

            printer.accept("\nNode ID: " + n.id() + " " + n.addresses() + "\nConsistent ID: " + n.consistentId() + "\n");

            if (printExceptionMessages) {
                String msg = e.getValue().getMessage();

                printer.accept("Exception: " + e.getValue().getClass().getCanonicalName() + "\n");
                printer.accept(msg == null ? "" : msg + "\n");
            }
        }
    }

    /**
     * Print partitions which were skipped.
     *
     * @param printer Consumer for printing.
     * @param map Partitions storage.
     * @param partitionState Partition state.
     */
    private void printSkippedPartitions(
        Consumer<String> printer,
        Map<PartitionKey, List<PartitionHashRecord>> map,
        String partitionState
    ) {
        if (!map.isEmpty()) {
            printer.accept("Verification was skipped for " + map.size() + " " + partitionState + " partitions:\n");

            for (Map.Entry<PartitionKey, List<PartitionHashRecord>> entry : map.entrySet()) {
                printer.accept("Skipped partition: " + entry.getKey() + "\n");

                printer.accept("Partition instances: " + entry.getValue() + "\n");
            }

            printer.accept("\n");
        }
    }

    /** */
    private void printConflicts(Consumer<String> printer) {
        int cntrConflictsSize = counterConflicts().size();
        int hashConflictsSize = hashConflicts().size();

        printer.accept("The check procedure has failed, conflict partitions has been found: [" +
            "counterConflicts=" + cntrConflictsSize + ", hashConflicts=" + hashConflictsSize
            + (txHashConflicts.isEmpty() ? "" : ", txHashConflicts=" + txHashConflicts.size())
            + (partiallyCommittedTxs.isEmpty() ? "" : ", partiallyCommittedSize=" + partiallyCommittedTxs.size())
            + "]" + nl());

        Set<PartitionKey> allConflicts = new HashSet<>();

        if (!cntrConflicts.isEmpty()) {
            allConflicts.addAll(counterConflicts().keySet());

            printer.accept("Update counter conflicts:" + nl());

            for (Map.Entry<PartitionKey, List<PartitionHashRecord>> entry : counterConflicts().entrySet()) {
                printer.accept("Conflict partition: " + entry.getKey() + nl());

                printer.accept("Partition instances: " + entry.getValue() + nl());
            }

            printer.accept(nl());
        }

        if (!hashConflicts.isEmpty()) {
            allConflicts.addAll(hashConflicts().keySet());

            printer.accept("Hash conflicts:" + nl());

            for (Map.Entry<PartitionKey, List<PartitionHashRecord>> entry : hashConflicts().entrySet()) {
                printer.accept("Conflict partition: " + entry.getKey() + nl());

                printer.accept("Partition instances: " + entry.getValue() + nl());
            }
        }

        if (!txHashConflicts.isEmpty()) {
            printer.accept("Transactions hashes conflicts:" + nl());

            for (List<TransactionsHashRecord> conf : txHashConflicts)
                printer.accept("Conflict nodes: " + conf + nl());
        }

        if (!partiallyCommittedTxs.isEmpty()) {
            printer.accept("Partially committed transactions:" + nl());

            for (Map.Entry<ClusterNode, Collection<GridCacheVersion>> entry : partiallyCommittedTxs.entrySet()) {
                printer.accept("Node: " + entry.getKey() + nl());

                printer.accept("Transactions: " + entry.getValue() + nl());
            }
        }

        printer.accept(nl());
        printer.accept("Total:" + nl());

        Map<String, TreeSet<Integer>> conflictsSummary = allConflicts.stream()
            .collect(Collectors.groupingBy(
                PartitionKey::groupName,
                Collectors.mapping(
                    PartitionKey::partitionId,
                    Collectors.toCollection(TreeSet::new))));

        for (Map.Entry<String, TreeSet<Integer>> grpConflicts : conflictsSummary.entrySet()) {
            printer.accept(String.format("%s (%d)%s", grpConflicts.getKey(), grpConflicts.getValue().size(), nl()));

            String partsStr = grpConflicts.getValue().stream()
                .map(String::valueOf)
                .collect(Collectors.joining(","));

            printer.accept(partsStr + nl());

            printer.accept(nl());
        }
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        IdleVerifyResult v = (IdleVerifyResult)o;

        return Objects.equals(cntrConflicts, v.cntrConflicts) && Objects.equals(hashConflicts, v.hashConflicts) &&
            Objects.equals(movingPartitions, v.movingPartitions) && Objects.equals(lostPartitions, v.lostPartitions) &&
            Objects.equals(exceptions, v.exceptions) && Objects.equals(txHashConflicts, v.txHashConflicts) &&
            Objects.equals(partiallyCommittedTxs, v.partiallyCommittedTxs);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(
            cntrConflicts,
            hashConflicts,
            movingPartitions,
            lostPartitions,
            exceptions,
            txHashConflicts,
            partiallyCommittedTxs);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IdleVerifyResult.class, this);
    }

    /** */
    private static <K, V> Map<K, V> notNullFinalMap(@Nullable Map<K, V> map) {
        return map == null ? Collections.emptyMap() : Collections.unmodifiableMap(map);
    }

    /** */
    private static <V> List<V> notNullFinalList(@Nullable List<V> lst) {
        return lst == null ? Collections.emptyList() : Collections.unmodifiableList(lst);
    }

    /**
     * Builds result holding errors only.
     *
     * @see #builder()
     */
    public static IdleVerifyResult ofErrors(Map<ClusterNode, Exception> exceptions) {
        return new IdleVerifyResult(null, null, null, null, null, null, exceptions);
    }

    /** @return A fresh result builder. */
    public static Builder builder() {
        return new Builder();
    }

    /** Builder of {@link IdleVerifyResult}. Is not thread-safe. */
    public static final class Builder {
        /** */
        private final IdleVerifyResult result = new IdleVerifyResult();

        /** Incremental snapshot transactions records per consistent id. */
        private @Nullable Map<Object, Map<Object, TransactionsHashRecord>> incrTxHashRecords;

        /** */
        private Builder() {
            // No-op.
        }

        /** Builds final {@link IdleVerifyResult}. */
        public IdleVerifyResult build() {
            // Add all missed incremental pairs to the conflicts.
            if (!F.isEmpty(incrTxHashRecords))
                incrTxHashRecords.values().stream().flatMap(e -> e.values().stream()).forEach(e -> addTxConflicts(F.asList(e, null)));

            if (F.isEmpty(result.hashConflicts))
                return result;

            // Actual hash conflicts is exist.
            Map<PartitionKey, List<PartitionHashRecord>> hashConflicts = null;
            Map<PartitionKey, List<PartitionHashRecord>> cntrConflicts = null;
            Map<PartitionKey, List<PartitionHashRecord>> movingPartitions = null;
            Map<PartitionKey, List<PartitionHashRecord>> lostPartitions = null;

            // The hash conflicts mas was used as a temporary storage of partitions hash records. Recalculating now.
            for (Map.Entry<PartitionKey, List<PartitionHashRecord>> e : result.hashConflicts.entrySet()) {
                Integer partHash = null;
                Integer partVerHash = null;
                Object updateCntr = null;

                for (PartitionHashRecord record : e.getValue()) {
                    if (record.partitionState() == PartitionHashRecord.PartitionState.MOVING) {
                        if (movingPartitions == null)
                            movingPartitions = new HashMap<>();

                        movingPartitions.computeIfAbsent(e.getKey(), k -> new ArrayList<>()).add(record);

                        continue;
                    }

                    if (record.partitionState() == PartitionHashRecord.PartitionState.LOST) {
                        if (lostPartitions == null)
                            lostPartitions = new HashMap<>();

                        lostPartitions.computeIfAbsent(e.getKey(), k -> new ArrayList<>())
                            .add(record);

                        continue;
                    }

                    if (partHash == null) {
                        partHash = record.partitionHash();
                        partVerHash = record.partitionVersionsHash();

                        updateCntr = record.updateCounter();
                    }
                    else {
                        if (!Objects.equals(record.updateCounter(), updateCntr)) {
                            if (cntrConflicts == null)
                                cntrConflicts = new HashMap<>();

                            cntrConflicts.putIfAbsent(e.getKey(), e.getValue());
                        }

                        if (record.partitionHash() != partHash || record.partitionVersionsHash() != partVerHash) {
                            if (hashConflicts == null)
                                hashConflicts = new HashMap<>();

                            hashConflicts.putIfAbsent(e.getKey(), e.getValue());
                        }
                    }
                }
            }

            // Set actual hash conflicts or clear the temporary partition hash records.
            result.hashConflicts = hashConflicts == null ? Collections.emptyMap() : hashConflicts;

            if (cntrConflicts != null)
                result.cntrConflicts = cntrConflicts;

            if (movingPartitions != null)
                result.movingPartitions = movingPartitions;

            if (lostPartitions != null)
                result.lostPartitions = lostPartitions;

            return result;
        }

        /** Stores an exception if none is aready set for certain node. */
        public Builder addException(ClusterNode node, Exception e) {
            assert e != null;

            if (result.exceptions == Collections.EMPTY_MAP)
                result.exceptions = new HashMap<>();

            result.exceptions.putIfAbsent(node, e);

            return this;
        }

        /** Stores map of partition hashes per partition key. */
        public void addPartitionHashes(Map<PartitionKey, PartitionHashRecord> newHashes) {
            // The hash conflicts is used as a temporary storage. Actual conflicts are calculated at the building.
            newHashes.forEach((key, hash) -> {
                if (result.hashConflicts == Collections.EMPTY_MAP)
                    result.hashConflicts = new HashMap<>();

                result.hashConflicts.compute(key, (key0, hashes0) -> {
                    if (hashes0 == null)
                        hashes0 = new ArrayList<>();

                    hashes0.add(hash);

                    return hashes0;
                });
            });
        }

        /** Stores incremental snapshot transaction hash records of a certain node. */
        public void addIncrementalHashRecords(ClusterNode node, Map<Object, TransactionsHashRecord> res) {
            if (incrTxHashRecords == null)
                incrTxHashRecords = new HashMap<>();

            assert incrTxHashRecords.get(node.consistentId()) == null;

            incrTxHashRecords.put(node.consistentId(), res);

            Iterator<Map.Entry<Object, TransactionsHashRecord>> resIt = res.entrySet().iterator();

            while (resIt.hasNext()) {
                Map.Entry<Object, TransactionsHashRecord> nodeTxHash = resIt.next();

                Map<Object, TransactionsHashRecord> prevNodeTxHash = incrTxHashRecords.get(nodeTxHash.getKey());

                if (prevNodeTxHash != null) {
                    TransactionsHashRecord hash = nodeTxHash.getValue();
                    TransactionsHashRecord prevHash = prevNodeTxHash.remove(hash.localConsistentId());

                    if (prevHash == null || prevHash.transactionHash() != hash.transactionHash())
                        addTxConflicts(F.asList(hash, prevHash));

                    resIt.remove();
                }
            }
        }

        /** Stores transaction conflicts. */
        private Builder addTxConflicts(List<TransactionsHashRecord> newTxConflicts) {
            if (result.txHashConflicts == Collections.EMPTY_LIST)
                result.txHashConflicts = new ArrayList<>();

            result.txHashConflicts.add(newTxConflicts);

            return this;
        }

        /** Stores partially commited transactions of a certain node. */
        public Builder addPartiallyCommited(ClusterNode node, Collection<GridCacheVersion> newVerisons) {
            if (result.partiallyCommittedTxs == Collections.EMPTY_MAP)
                result.partiallyCommittedTxs = new HashMap<>();

            result.partiallyCommittedTxs.compute(node, (node0, versions0) -> {
                if (versions0 == null)
                    versions0 = new ArrayList<>();

                versions0.addAll(newVerisons);

                return versions0;
            });

            return this;
        }
    }
}
