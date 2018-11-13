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
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
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
    private Map<PartitionKeyV2, List<PartitionHashRecordV2>> cntrConflicts;

    /** Hash conflicts. */
    private Map<PartitionKeyV2, List<PartitionHashRecordV2>> hashConflicts;

    /** Moving partitions. */
    private Map<PartitionKeyV2, List<PartitionHashRecordV2>> movingPartitions;

    /**
     * @param cntrConflicts Counter conflicts.
     * @param hashConflicts Hash conflicts.
     * @param movingPartitions Moving partitions.
     */
    public IdleVerifyResultV2(
        Map<PartitionKeyV2, List<PartitionHashRecordV2>> cntrConflicts,
        Map<PartitionKeyV2, List<PartitionHashRecordV2>> hashConflicts,
        Map<PartitionKeyV2, List<PartitionHashRecordV2>> movingPartitions
    ) {
        this.cntrConflicts = cntrConflicts;
        this.hashConflicts = hashConflicts;
        this.movingPartitions = movingPartitions;
    }

    /**
     * Default constructor for Externalizable.
     */
    public IdleVerifyResultV2() {
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeMap(out, cntrConflicts);
        U.writeMap(out, hashConflicts);
        U.writeMap(out, movingPartitions);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer,
        ObjectInput in) throws IOException, ClassNotFoundException {
        cntrConflicts = U.readMap(in);
        hashConflicts = U.readMap(in);
        movingPartitions = U.readMap(in);
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
        return movingPartitions;
    }

    /**
     * @return <code>true</code> if any conflicts were discovered during idle_verify check.
     */
    public boolean hasConflicts() {
        return !F.isEmpty(hashConflicts()) || !F.isEmpty(counterConflicts());
    }

    /**
     * Print formatted result to given printer.
     *
     * @param printer Consumer for handle formatted result.
     */
    public void print(Consumer<String> printer) {
        if (!hasConflicts())
            printer.accept("idle_verify check has finished, no conflicts have been found.\n");
        else {
            int cntrConflictsSize = counterConflicts().size();
            int hashConflictsSize = hashConflicts().size();

            printer.accept("idle_verify check has finished, found " + (cntrConflictsSize + hashConflictsSize) +
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

                printer.accept("\n");
            }
        }

        if (!F.isEmpty(movingPartitions())) {
            printer.accept("Verification was skipped for " + movingPartitions().size() + " MOVING partitions:\n");

            for (Map.Entry<PartitionKeyV2, List<PartitionHashRecordV2>> entry : movingPartitions().entrySet()) {
                printer.accept("Rebalancing partition: " + entry.getKey() + "\n");

                printer.accept("Partition instances: " + entry.getValue() + "\n");
            }

            printer.accept("\n");
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IdleVerifyResultV2.class, this);
    }
}
