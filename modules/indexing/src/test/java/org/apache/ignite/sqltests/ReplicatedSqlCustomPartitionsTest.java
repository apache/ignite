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

package org.apache.ignite.sqltests;

import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.junit.Test;

/**
 * Includes all base sql test plus tests that make sense in replicated mode with a non-default number of partitions.
 */
public class ReplicatedSqlCustomPartitionsTest extends ReplicatedSqlTest {
    /** Test partitions count. */
    private static final int NUM_OF_PARTITIONS = 509;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(
                new CacheConfiguration("partitioned" + NUM_OF_PARTITIONS + "*")
                    .setAffinity(new RendezvousAffinityFunction(false, NUM_OF_PARTITIONS)),
                new CacheConfiguration("replicated" + NUM_OF_PARTITIONS + "*")
                    .setCacheMode(CacheMode.REPLICATED)
                    .setAffinity(new RendezvousAffinityFunction(false, NUM_OF_PARTITIONS))
            );
    }

    /**
     * Create and fill common tables in replicated mode.
     * Also create additional department table in partitioned mode to
     * test mixed partitioned/replicated scenarios.
     */
    @Override protected void setupData() {
        createTables("template=replicated" + NUM_OF_PARTITIONS);

        fillCommonData();

        createDepartmentTable(DEP_PART_TAB, "template=partitioned" + NUM_OF_PARTITIONS);

        fillDepartmentTable(DEP_PART_TAB);
    }

    /**
     * Check LEFT JOIN with collocated data of replicated and partitioned tables.
     * This test relies on having the same number of partitions in replicated and partitioned caches
     */
    @Test
    public void testLeftJoinReplicatedPartitioned() {
        checkLeftJoinEmployeeDepartment(DEP_PART_TAB);
    }

    /**
     * Check RIGHT JOIN with collocated data of partitioned and replicated tables.
     */
    @Test
    public void testRightJoinPartitionedReplicated() {
        checkRightJoinDepartmentEmployee(DEP_PART_TAB);
    }
}
