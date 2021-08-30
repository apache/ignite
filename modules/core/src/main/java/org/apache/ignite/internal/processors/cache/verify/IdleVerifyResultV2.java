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
package org.apache.ignite.internal.processors.cache.verify;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Encapsulates result of {@link VerifyBackupPartitionsTaskV2}.
 */
public class IdleVerifyResultV2 extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Counter conflicts. */
    @GridToStringInclude
    private Map<PartitionKeyV2, List<PartitionHashRecordV2>> cntrConflicts = new HashMap<>();

    /** Hash conflicts. */
    @GridToStringInclude
    private Map<PartitionKeyV2, List<PartitionHashRecordV2>> hashConflicts = new HashMap<>();

    /** Moving partitions. */
    @GridToStringInclude
    private Map<PartitionKeyV2, List<PartitionHashRecordV2>> movingPartitions = new HashMap<>();

    /** Lost partitions. */
    @GridToStringInclude
    private Map<PartitionKeyV2, List<PartitionHashRecordV2>> lostPartitions = new HashMap<>();

    /** Exceptions. */
    @GridToStringInclude
    private Map<ClusterNode, Exception> exceptions;

    /**
     * Default constructor for Externalizable.
     */
    public IdleVerifyResultV2() {
    }

    /**
     * @param exceptions Occurred exceptions.
     */
    public IdleVerifyResultV2(Map<ClusterNode, Exception> exceptions) {
        this.exceptions = exceptions;
    }

    /**
     * @param clusterHashes Map of cluster partition hashes.
     * @param exceptions Exceptions on each cluster node.
     */
    public IdleVerifyResultV2(
        Map<PartitionKeyV2, List<PartitionHashRecordV2>> clusterHashes,
        Map<ClusterNode, Exception> exceptions
    ) {
        for (Map.Entry<PartitionKeyV2, List<PartitionHashRecordV2>> e : clusterHashes.entrySet()) {
            Integer partHash = null;
            Long updateCntr = null;

            for (PartitionHashRecordV2 record : e.getValue()) {
                if (record.partitionState() == PartitionHashRecordV2.PartitionState.MOVING) {
                    movingPartitions.computeIfAbsent(e.getKey(), k -> new ArrayList<>())
                        .add(record);

                    continue;
                }

                if (record.partitionState() == PartitionHashRecordV2.PartitionState.LOST) {
                    lostPartitions.computeIfAbsent(e.getKey(), k -> new ArrayList<>())
                        .add(record);

                    continue;
                }

                if (partHash == null) {
                    partHash = record.partitionHash();

                    updateCntr = record.updateCounter();
                }
                else {
                    if (record.updateCounter() != updateCntr)
                        cntrConflicts.putIfAbsent(e.getKey(), e.getValue());

                    if (record.partitionHash() != partHash)
                        hashConflicts.putIfAbsent(e.getKey(), e.getValue());
                }
            }
        }

        this.exceptions = exceptions;
    }

    /** {@inheritDoc} */
    @Override public byte getProtocolVersion() {
        return V3;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeMap(out, cntrConflicts);
        U.writeMap(out, hashConflicts);
        U.writeMap(out, movingPartitions);
        U.writeMap(out, exceptions);
        U.writeMap(out, lostPartitions);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer,
        ObjectInput in) throws IOException, ClassNotFoundException {
        cntrConflicts = U.readMap(in);
        hashConflicts = U.readMap(in);
        movingPartitions = U.readMap(in);

        if (protoVer >= V2)
            exceptions = U.readMap(in);

        if (protoVer >= V3)
            lostPartitions = U.readMap(in);
    }

    /**
     * @return Counter conflicts.
     */
    public Map<PartitionKeyV2, List<PartitionHashRecordV2>> counterConflicts() {
        return cntrConflicts;
    }

    /**
     * @return Hash conflicts.
     */
    public Map<PartitionKeyV2, List<PartitionHashRecordV2>> hashConflicts() {
        return hashConflicts;
    }

    /**
     * @return Moving partitions.
     */
    public Map<PartitionKeyV2, List<PartitionHashRecordV2>> movingPartitions() {
        return Collections.unmodifiableMap(movingPartitions);
    }

    /**
     * @return Lost partitions.
     */
    public Map<PartitionKeyV2, List<PartitionHashRecordV2>> lostPartitions() {
        return lostPartitions;
    }

    /**
     * @return {@code true} if any conflicts were discovered during the check.
     */
    public boolean hasConflicts() {
        return !F.isEmpty(hashConflicts()) || !F.isEmpty(counterConflicts());
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
        boolean noMatchingCaches = false;

        boolean succeeded = true;

        for (Exception e : exceptions.values()) {
            if (e instanceof NoMatchingCachesException) {
                noMatchingCaches = true;
                succeeded = false;

                break;
            }
        }

        if (succeeded) {
            if (!F.isEmpty(exceptions)) {
                int size = exceptions.size();

                printer.accept("The check procedure failed on " + size + " node" + (size == 1 ? "" : "s") + ".\n");
            }

            if (!hasConflicts())
                printer.accept("The check procedure has finished, no conflicts have been found.\n");
            else
                printConflicts(printer);

            Map<PartitionKeyV2, List<PartitionHashRecordV2>> moving = movingPartitions();

            if (!moving.isEmpty())
                printer.accept("Possible results are not full due to rebalance still in progress." + U.nl());

            printSkippedPartitions(printer, moving, "MOVING");
            printSkippedPartitions(printer, lostPartitions(), "LOST");
        }
        else {
            printer.accept("\nThe check procedure failed.\n");

            if (noMatchingCaches)
                printer.accept("\nThere are no caches matching given filter options.\n");
        }

        if (!F.isEmpty(exceptions())) {
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
        Map<PartitionKeyV2, List<PartitionHashRecordV2>> map,
        String partitionState
    ) {
        if (!F.isEmpty(map)) {
            printer.accept("Verification was skipped for " + map.size() + " " + partitionState + " partitions:\n");

            for (Map.Entry<PartitionKeyV2, List<PartitionHashRecordV2>> entry : map.entrySet()) {
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

        printer.accept("The check procedure has finished, found " + (cntrConflictsSize + hashConflictsSize) +
            " conflict partitions: [counterConflicts=" + cntrConflictsSize + ", hashConflicts=" +
            hashConflictsSize + "]\n");

        if (!F.isEmpty(counterConflicts())) {
            printer.accept("Update counter conflicts:\n");

            for (Map.Entry<PartitionKeyV2, List<PartitionHashRecordV2>> entry : counterConflicts().entrySet()) {
                printer.accept("Conflict partition: " + entry.getKey() + "\n");

                printer.accept("Partition instances: " + entry.getValue() + "\n");
            }

            printer.accept("\n");
        }

        if (!F.isEmpty(hashConflicts())) {
            printer.accept("Hash conflicts:\n");

            for (Map.Entry<PartitionKeyV2, List<PartitionHashRecordV2>> entry : hashConflicts().entrySet()) {
                printer.accept("Conflict partition: " + entry.getKey() + "\n");

                printer.accept("Partition instances: " + entry.getValue() + "\n");
            }
        }

        printer.accept("\n");
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        IdleVerifyResultV2 v2 = (IdleVerifyResultV2)o;

        return Objects.equals(cntrConflicts, v2.cntrConflicts) && Objects.equals(hashConflicts, v2.hashConflicts) &&
            Objects.equals(movingPartitions, v2.movingPartitions) && Objects.equals(lostPartitions, v2.lostPartitions) &&
            Objects.equals(exceptions, v2.exceptions);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(cntrConflicts, hashConflicts, movingPartitions, lostPartitions, exceptions);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IdleVerifyResultV2.class, this);
    }
}
