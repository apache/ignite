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
import java.util.Set;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Partition reconciliation task arguments.
 */
public class VisorPartitionReconciliationTaskArg extends IgniteDataTransferObject {
    /**
     *
     */
    private static final long serialVersionUID = 0L;

    /** Caches. */
    private Set<String> caches;

    /** If {@code true} - Partition Reconciliation&Fix: update from Primary partition. */
    private boolean repair;

    /** Print result to locOutput. */
    private boolean locOutput;

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
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public VisorPartitionReconciliationTaskArg(Set<String> caches, boolean repair, boolean includeSensitive, boolean locOutput,
        int parallelism, int batchSize, int recheckAttempts, RepairAlgorithm repairAlg, int recheckDelay) {
        this.caches = caches;
        this.includeSensitive = includeSensitive;
        this.locOutput = locOutput;
        this.repair = repair;
        this.parallelism = parallelism;
        this.batchSize = batchSize;
        this.recheckAttempts = recheckAttempts;
        this.repairAlg = repairAlg;
        this.recheckDelay = recheckDelay;
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
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in)
        throws IOException, ClassNotFoundException {

        caches = U.readSet(in);

        repair = in.readBoolean();

        includeSensitive = in.readBoolean();

        locOutput = in.readBoolean();

        parallelism = in.readInt();

        batchSize = in.readInt();

        recheckAttempts = in.readInt();

        repairAlg = RepairAlgorithm.fromOrdinal(in.readByte());

        recheckDelay = in.readInt();
    }

    /**
     * @return Caches.
     */
    public Set<String> caches() {
        return caches;
    }

    /**
     * @return If  - Partition Reconciliation&Fix: update from Primary partition.
     */
    public boolean repair() {
        return repair;
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
     * @return If  - print data to result with sensitive information: keys and values.
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
            return new VisorPartitionReconciliationTaskArg(
                caches, repair, includeSensitive, locOutput, parallelism, batchSize, recheckAttempts, repairAlg, recheckDelay);
        }

        /**
         * @param caches New caches.
         */
        public Builder caches(Set<String> caches) {
            this.caches = caches;

            return this;
        }

        /**
         * @param repair New if  - Partition Reconciliation&Fix: update from Primary partition.
         */
        public Builder repair(boolean repair) {
            this.repair = repair;

            return this;
        }

        /**
         * @param locOutput New print result to locOutput.
         */
        public Builder locOutput(boolean locOutput) {
            this.locOutput = locOutput;

            return this;
        }

        /**
         * @param includeSensitive New if  - print data to result with sensitive information: keys and values.
         */
        public Builder includeSensitive(boolean includeSensitive) {
            this.includeSensitive = includeSensitive;

            return this;
        }

        /**
         * @param parallelism Maximum number of threads that can be involved in reconciliation activities.
         */
        public Builder parallelism(int parallelism) {
            this.parallelism = parallelism;

            return this;
        }

        /**
         * @param batchSize New amount of keys to retrieve within one job.
         */
        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;

            return this;
        }

        /**
         * @param recheckAttempts New amount of potentially inconsistent keys recheck attempts.
         */
        public Builder recheckAttempts(int recheckAttempts) {
            this.recheckAttempts = recheckAttempts;

            return this;
        }

        /**
         * @param repairAlg New specifies which fix algorithm to use: options  while repairing doubtful keys.
         */
        public Builder repairAlg(RepairAlgorithm repairAlg) {
            this.repairAlg = repairAlg;

            return this;
        }

        /**
         * @param recheckDelay Recheck delay seconds.
         */
        public Builder recheckDelay(int recheckDelay) {
            this.recheckDelay = recheckDelay;

            return this;
        }
    }
}
