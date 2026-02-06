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

package org.apache.ignite.internal.management.consistency;

import org.apache.ignite.cache.ReadRepairStrategy;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.EnumDescription;

/** */
public class ConsistencyRepairCommandArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    @Order(value = 0)
    @Argument(description = "Cache to be checked/repaired")
    String cache;

    /** */
    @Order(value = 1)
    @Argument(description = "Cache's partition to be checked/repaired", example = "partition")
    int[] partitions;

    /** Strategy. */
    @Order(value = 2)
    @Argument(description = "Repair strategy")
    @EnumDescription(
        names = {
            "LWW",
            "PRIMARY",
            "RELATIVE_MAJORITY",
            "REMOVE",
            "CHECK_ONLY"
        },
        descriptions = {
            "Last write (the newest entry) wins",
            "Value from the primary node wins",
            "The relative majority, any value found more times than any other wins",
            "Inconsistent entries will be removed",
            "Only check will be performed"
        }
    )
    ReadRepairStrategy strategy;

    /** */
    @Order(value = 3)
    @Argument(description = "Run concurrently on each node", optional = true)
    boolean parallel;

    /** */
    public void ensureParams() {
        // see https://issues.apache.org/jira/browse/IGNITE-15316
        if (parallel && strategy != ReadRepairStrategy.CHECK_ONLY) {
            throw new UnsupportedOperationException(
                "Parallel mode currently allowed only when CHECK_ONLY strategy is chosen.");
        }
    }

    /** */
    public int[] partitions() {
        return partitions;
    }

    /** */
    public void partitions(int[] partition) {
        this.partitions = partition;
    }

    /** */
    public String cache() {
        return cache;
    }

    /** */
    public void cache(String cacheName) {
        this.cache = cacheName;
    }

    /** */
    public ReadRepairStrategy strategy() {
        return strategy;
    }

    /** */
    public void strategy(ReadRepairStrategy strategy) {
        this.strategy = strategy;
        ensureParams();
    }

    /** */
    public boolean parallel() {
        return parallel;
    }

    /** */
    public void parallel(boolean parallel) {
        this.parallel = parallel;
        ensureParams();
    }
}
