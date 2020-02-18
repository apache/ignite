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

package org.apache.ignite.internal.commandline.cache.argument;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.internal.commandline.argument.CommandArg;
import org.apache.ignite.internal.commandline.cache.CacheSubcommands;
import org.apache.ignite.internal.processors.cache.verify.RepairAlgorithm;

/**
 * {@link CacheSubcommands#PARTITION_RECONCILIATION} command arguments.
 */
public enum PartitionReconciliationCommandArg implements CommandArg {
    /**
     * If present - Partition Reconciliation&Fix: update from Primary partition. Specifies which fix algorithm to use
     * while repairing doubtful keys: options {@code PartitionReconciliationRepairMeta.RepairAlg}.
     */
    REPAIR("--repair", RepairAlgorithm.defaultValue()),

    /** If {@code true} - print data to result with sensitive information: keys and values. */
    INCLUDE_SENSITIVE("--include-sensitive", Boolean.FALSE),

    /** Maximum number of threads that can be involved in reconciliation activities. */
    PARALLELISM("--parallelism", 0),

    /** Amount of keys to retrieve within one job. */
    BATCH_SIZE("--batch-size", 1000),

    /** Amount of potentially inconsistent keys recheck attempts. */
    RECHECK_ATTEMPTS("--recheck-attempts", 2),

    /** Print result to console. Specifies whether to print result to console or file. Hide parameter. */
    LOCAL_OUTPUT("--local-output", Boolean.FALSE),

    /** Recheck delay seconds. */
    RECHECK_DELAY("--recheck-delay", 5);

    /** Option name. */
    private final String name;

    /** Default value. */
    private Object dfltVal;

    /**
     *
     */
    PartitionReconciliationCommandArg(String name, Object dfltVal) {
        this.name = name;
        this.dfltVal = dfltVal;
    }

    /**
     * @return List of args.
     */
    public static List<String> commands() {
        return Arrays.stream(PartitionReconciliationCommandArg.values())
            .map(PartitionReconciliationCommandArg::argName)
            .collect(Collectors.toList());
    }

    /** {@inheritDoc} */
    @Override public String argName() {
        return name;
    }

    /**
     * @return Default value.
     */
    public Object defaultValue() {
        return dfltVal;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return name;
    }
}
