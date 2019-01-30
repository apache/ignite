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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.commandline.cache.CacheCommand.IDLE_VERIFY;

/**
 * Encapsulates result of {@link VerifyBackupPartitionsTaskV2}.
 */
public class IdleVerifyResultV2 extends VisorDataTransferObject {
    /** */
    public static final String IDLE_VERIFY_FILE_PREFIX = IDLE_VERIFY + "-";

    /** Time formatter for log file name. */
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH-mm-ss_SSS");

    /** */
    private static final long serialVersionUID = 0L;

    /** Counter conflicts. */
    private Map<PartitionKeyV2, List<PartitionHashRecordV2>> cntrConflicts;

    /** Hash conflicts. */
    private Map<PartitionKeyV2, List<PartitionHashRecordV2>> hashConflicts;

    /** Moving partitions. */
    private Map<PartitionKeyV2, List<PartitionHashRecordV2>> movingPartitions;

    /** Exceptions. */
    private Map<ClusterNode, Exception> exceptions;

    /**
     * @param cntrConflicts Counter conflicts.
     * @param hashConflicts Hash conflicts.
     * @param movingPartitions Moving partitions.
     * @param exceptions Occured exceptions.
     */
    public IdleVerifyResultV2(
        Map<PartitionKeyV2, List<PartitionHashRecordV2>> cntrConflicts,
        Map<PartitionKeyV2, List<PartitionHashRecordV2>> hashConflicts,
        Map<PartitionKeyV2, List<PartitionHashRecordV2>> movingPartitions,
        Map<ClusterNode, Exception> exceptions
    ) {
        this.cntrConflicts = cntrConflicts;
        this.hashConflicts = hashConflicts;
        this.movingPartitions = movingPartitions;
        this.exceptions = exceptions;
    }

    /**
     * Default constructor for Externalizable.
     */
    public IdleVerifyResultV2() {
    }

    /** {@inheritDoc} */
    @Override public byte getProtocolVersion() {
        return V2;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeMap(out, cntrConflicts);
        U.writeMap(out, hashConflicts);
        U.writeMap(out, movingPartitions);
        U.writeMap(out, exceptions);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer,
        ObjectInput in) throws IOException, ClassNotFoundException {
        cntrConflicts = U.readMap(in);
        hashConflicts = U.readMap(in);
        movingPartitions = U.readMap(in);

        if (protoVer >= V2)
            exceptions = U.readMap(in);
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
     * @return Exceptions on nodes.
     */
    public Map<ClusterNode, Exception> exceptions() {
        return exceptions;
    }

    /**
     * Print formatted result to given printer. If exceptions presented exception messages will be written to log file.
     *
     * @param printer Consumer for handle formatted result.
     * @return Path to log file if exceptions presented and {@code null} otherwise.
     */
    public @Nullable String print(Consumer<String> printer) {
        print(printer, false);

        if (!F.isEmpty(exceptions)) {
            File f = new File(IDLE_VERIFY_FILE_PREFIX + LocalDateTime.now().format(TIME_FORMATTER) + ".txt");

            try (PrintWriter pw = new PrintWriter(f)) {
                print(pw::write, true);

                pw.flush();

                printer.accept("See log for additional information. " + f.getAbsolutePath() + "\n");

                return f.getAbsolutePath();
            }
            catch (FileNotFoundException e) {
                printer.accept("Can't write exceptions to file " + f.getAbsolutePath() + " " + e.getMessage() + "\n");

                e.printStackTrace();
            }
        }

        return null;
    }

    /** */
    private void print(Consumer<String> printer, boolean printExceptionMessages) {
        if (!F.isEmpty(exceptions)) {
            int size = exceptions.size();

            printer.accept("idle_verify failed on " + size + " node" + (size == 1 ? "" : "s") + ".\n");
        }

        if (!hasConflicts())
            printer.accept("idle_verify check has finished, no conflicts have been found.\n");
        else
            printConflicts(printer);

        if (!F.isEmpty(movingPartitions())) {
            printer.accept("Verification was skipped for " + movingPartitions().size() + " MOVING partitions:\n");

            for (Map.Entry<PartitionKeyV2, List<PartitionHashRecordV2>> entry : movingPartitions().entrySet()) {
                printer.accept("Rebalancing partition: " + entry.getKey() + "\n");

                printer.accept("Partition instances: " + entry.getValue() + "\n");
            }

            printer.accept("\n");
        }

        if (!F.isEmpty(exceptions())) {
            printer.accept("Idle verify failed on nodes:\n");

            for (Map.Entry<ClusterNode, Exception> e : exceptions().entrySet()) {
                ClusterNode n = e.getKey();

                printer.accept("Node ID: " + n.id() + " " + n.addresses() + " consistent ID: " + n.consistentId() + "\n");

                if (printExceptionMessages) {
                    printer.accept("Exception message:" + "\n");

                    printer.accept(e.getValue().getMessage() + "\n");
                }
            }
        }
    }

    /** */
    private void printConflicts(Consumer<String> printer) {
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IdleVerifyResultV2.class, this);
    }
}
