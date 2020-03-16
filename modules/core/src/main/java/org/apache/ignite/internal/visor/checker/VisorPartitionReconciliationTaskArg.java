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

package org.apache.ignite.internal.visor.checker;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Partition reconciliation task arguments.
 */
public class VisorPartitionReconciliationTaskArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Caches. */
    private Set<String> caches;

    /** If {@code true} - Partition Reconciliation&Fix: update from Primary partition. */
    private boolean repair;

    /** Print result to locOutput. */
    private boolean locOutput;

    /** Flag indicates that only subset of partitions should be checked and repaired. */
    private boolean fastCheck;

    /**
     * Collection of partitions which should be validated.
     * This collection can be {@code null}, this means that all partitions should be validated/repaired.
     * Mapping: Cache group id -> collection of partitions.
     */
    private Map<Integer, Set<Integer>> partsToRepair;

    /** If {@code true} - print data to result with sensitive information: keys and values. */
    private boolean includeSensitive;

    /** Maximum number of threads that can be involved in reconciliation activities. */
    private int parallelism;

    /** Amount of keys to retrieve within one job. */
    private int batchSize;

    /** Amount of potentially inconsistent keys recheck attempts. */
    private int recheckAttempts;

    /**
     * Specifies which fix algorithm to use: options {@code PartitionReconciliationRepairMeta.RepairAlg} while repairing
     * doubtful keys.
     */
    private RepairAlgorithm repairAlg;

    /** Recheck delay seconds. */
    private int recheckDelay;

    /**
     * Default constructor.
     */
    public VisorPartitionReconciliationTaskArg() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param caches List of cache names to be checked.
     * @param fastCheck If {@code true} then only partitions that did not pass validation
     *                  during the last partition map exchange will be checked and repaired.
     *                  Otherwise, all partitions will be taken into account.
     * @param repair Flag indicates that an inconsistency should be fixed in accordance with {@code repairAlg} parameter.
     * @param includeSensitive Flag indicates that the result should include sensitive information such as key and value.
     * @param locOutput Flag indicates that the result is printed to the console.
     * @param parallelism Maximum number of threads that can be involved in reconciliation activities.
     * @param batchSize Amount of keys to checked at one time.
     * @param recheckAttempts Amount of potentially inconsistent keys recheck attempts.
     * @param repairAlg Partition reconciliation repair algorithm to be used.
     * @param recheckDelay Recheck delay in seconds.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public VisorPartitionReconciliationTaskArg(
        Set<String> caches,
        boolean fastCheck,
        boolean repair,
        boolean includeSensitive,
        boolean locOutput,
        int parallelism,
        int batchSize,
        int recheckAttempts,
        RepairAlgorithm repairAlg,
        int recheckDelay
    ) {
        this.caches = caches;
        this.fastCheck = fastCheck;
        this.includeSensitive = includeSensitive;
        this.locOutput = locOutput;
        this.repair = repair;
        this.parallelism = parallelism;
        this.batchSize = batchSize;
        this.recheckAttempts = recheckAttempts;
        this.repairAlg = repairAlg;
        this.recheckDelay = recheckDelay;
    }

    /**
     * Creates a new instance of VisorPartitionReconciliationTaskArg initialized the given builder.
     *
     * @param b Reconciliation arguments builder.
     */
    public VisorPartitionReconciliationTaskArg(Builder b) {
        this(b.caches,
             b.fastCheck,
             b.repair,
             b.includeSensitive,
             b.locOutput,
             b.parallelism,
             b.batchSize,
             b.recheckAttempts,
             b.repairAlg,
             b.recheckDelay);

        if (b.partsToRepair != null) {
            partsToRepair = b.partsToRepair
                .entrySet()
                .stream()
                .collect(Collectors.toMap(e -> e.getKey(), e -> new HashSet<>(e.getValue())));
        }
    }

    /** {@inheritDoc} */
    @Override public byte getProtocolVersion() {
        return V2;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeCollection(out, caches);

        out.writeBoolean(repair);

        out.writeBoolean(includeSensitive);

        out.writeBoolean(locOutput);

        out.writeInt(parallelism);

        out.writeInt(batchSize);

        out.writeInt(recheckAttempts);

        U.writeEnum(out, repairAlg);

        out.writeInt(recheckDelay);

        // The following fields were added in V2 version.
        out.writeBoolean(fastCheck);

        U.writeIntKeyMap(out, partsToRepair);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {

        caches = U.readSet(in);

        repair = in.readBoolean();

        includeSensitive = in.readBoolean();

        locOutput = in.readBoolean();

        parallelism = in.readInt();

        batchSize = in.readInt();

        recheckAttempts = in.readInt();

        repairAlg = RepairAlgorithm.fromOrdinal(in.readByte());

        recheckDelay = in.readInt();

        if (protoVer >= V2) {
            fastCheck = in.readBoolean();

            partsToRepair = U.readIntKeyMap(in);
        }
    }

    /**
     * @return Caches to be checked.
     */
    public Set<String> caches() {
        return caches;
    }

    /**
     * @return {@code true} if an inconsistency should be fixed in accordance with {@link #repairAlg()}.
     */
    public boolean repair() {
        return repair;
    }

    /**
     * @return {@code true} if only partitions that did not pass validation during the last partition map exchange
     * will be checked and repaired.
     */
    public boolean fastCheck() {
        return fastCheck;
    }

    /**
     * @return Collection of partitions that should be checked and repaired.
     */
    public Map<Integer, Set<Integer>> partitionsToRepair() {
        return partsToRepair;
    }

    /**
     * @return Amount of keys to retrieve within one job.
     */
    public int batchSize() {
        return batchSize;
    }

    /**
     * @return Amount of potentially inconsistent keys recheck attempts.
     */
    public int recheckAttempts() {
        return recheckAttempts;
    }

    /**
     * @return {@code true} if the result should include sensitive information such as key and value.
     */
    public boolean includeSensitive() {
        return includeSensitive;
    }

    /**
     * @return {@code true} if print result to locOutput.
     */
    public boolean locOutput() {
        return locOutput;
    }

    /**
     * @return Specifies which fix algorithm to use: options  while repairing doubtful keys.
     */
    public RepairAlgorithm repairAlg() {
        return repairAlg;
    }

    /**
     * @return Maximum number of threads that can be involved in reconciliation activities.
     */
    public int parallelism() {
        return parallelism;
    }

    /**
     * @return Recheck delay seconds.
     */
    public int recheckDelay() {
        return recheckDelay;
    }

    /**
     * Builder class for test purposes.
     */
    public static class Builder {
        /** Caches. */
        private Set<String> caches;

        /** If {@code true} - Partition Reconciliation&Fix: update from Primary partition. */
        private boolean repair;

        /** Print result to locOutput. */
        private boolean locOutput;

        /** Flag indicates that only subset of partitions should be checked and repaired. */
        private boolean fastCheck;

        /**
         * Collection of partitions which should be validated.
         * This collection can be {@code null}, this means that all partitions should be validated/repaired.
         * Mapping: Cache group id -> collection of partitions.
         */
        private Map<Integer, Set<Integer>> partsToRepair;

        /** If {@code true} - print data to result with sensitive information: keys and values. */
        private boolean includeSensitive;

        /** Maximum number of threads that can be involved in reconciliation activities. */
        private int parallelism;

        /** Amount of keys to retrieve within one job. */
        private int batchSize;

        /** Amount of potentially inconsistent keys recheck attempts. */
        private int recheckAttempts;

        /**
         * Specifies which fix algorithm to use: options {@code PartitionReconciliationRepairMeta.RepairAlg} while
         * repairing doubtful keys.
         */
        private RepairAlgorithm repairAlg;

        /** Recheck delay seconds. */
        private int recheckDelay;

        /**
         * Default constructor.
         */
        public Builder() {
            caches = null;
            repair = false;
            locOutput = true;
            includeSensitive = true;
            fastCheck = false;
            parallelism = 4;
            batchSize = 100;
            recheckAttempts = 2;
            recheckDelay = 1;
            repairAlg = RepairAlgorithm.defaultValue();
        }

        /**
         * Copy constructor.
         *
         * @param cpFrom Argument to copy from.
         */
        public Builder(VisorPartitionReconciliationTaskArg cpFrom) {
            caches = cpFrom.caches;
            repair = cpFrom.repair;
            locOutput = cpFrom.locOutput;
            includeSensitive = cpFrom.includeSensitive;
            fastCheck = cpFrom.fastCheck;
            partsToRepair = cpFrom.partsToRepair;
            parallelism = cpFrom.parallelism;
            batchSize = cpFrom.batchSize;
            recheckAttempts = cpFrom.batchSize;
            recheckAttempts = cpFrom.recheckAttempts;
            recheckDelay = cpFrom.recheckDelay;
            repairAlg = cpFrom.repairAlg;
        }

        /**
         * Build metod.
         */
        public VisorPartitionReconciliationTaskArg build() {
            return new VisorPartitionReconciliationTaskArg(this);
        }

        /**
         * @param caches New caches.
         * @return Builder for chaning.
         */
        public Builder caches(Set<String> caches) {
            this.caches = caches;

            return this;
        }

        /**
         * @param repair New if  - Partition Reconciliation&Fix: update from Primary partition.
         * @return Builder for chaning.
         */
        public Builder repair(boolean repair) {
            this.repair = repair;

            return this;
        }

        /**
         * @param fastCheck Flag indicates that only partitions that did not pass validation
         *                  during the last partition map exchange will be checked and repaired.
         * @return Builder for chaning.
         */
        public Builder fastCheck(boolean fastCheck) {
            this.fastCheck = fastCheck;

            return this;
        }

        /**
         * @param partsToRepair  Collection of partitions that should be checked and repaired.
         * @return Builder for chaning.
         */
        public Builder partitionsToRepair(Map<Integer, Set<Integer>> partsToRepair) {
            this.partsToRepair = partsToRepair;

            return this;
        }

        /**
         * @param locOutput The result will be primted to output if {@code locOutput} equals to {@code true}.
         * @return Builder for chaning.
         */
        public Builder locOutput(boolean locOutput) {
            this.locOutput = locOutput;

            return this;
        }

        /**
         * @param includeSensitive If {@code true} then sensitive information is included into result.
         * @return Builder for chaning.
         */
        public Builder includeSensitive(boolean includeSensitive) {
            this.includeSensitive = includeSensitive;

            return this;
        }

        /**
         * @param parallelism Maximum number of threads that can be involved in reconciliation activities.
         * @return Builder for chaning.
         */
        public Builder parallelism(int parallelism) {
            this.parallelism = parallelism;

            return this;
        }

        /**
         * @param batchSize New amount of keys to retrieve within one job.
         * @return Builder for chaning.
         */
        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;

            return this;
        }

        /**
         * @param recheckAttempts New amount of potentially inconsistent keys recheck attempts.
         * @return Builder for chaning.
         */
        public Builder recheckAttempts(int recheckAttempts) {
            this.recheckAttempts = recheckAttempts;

            return this;
        }

        /**
         * @param repairAlg New specifies which fix algorithm to use: options  while repairing doubtful keys.
         * @return Builder for chaning.
         */
        public Builder repairAlg(RepairAlgorithm repairAlg) {
            this.repairAlg = repairAlg;

            return this;
        }

        /**
         * @param recheckDelay Recheck delay seconds.
         * @return Builder for chaning.
         */
        public Builder recheckDelay(int recheckDelay) {
            this.recheckDelay = recheckDelay;

            return this;
        }
    }
}
