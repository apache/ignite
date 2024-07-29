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

import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Includes all base sql test plus tests that make sense in replicated mode with a non-default number of partitions.
 */
public class ReplicatedSqlCustomPartitionsTest extends ReplicatedSqlTest {
    /** Test partitions count. */
    static final int NUM_OF_PARTITIONS = 509;

    /** */
    static final String DEP_PART_TAB_DIFF = "DepartmentPartDiff";

    /** */
    static final String DEP_PART_TAB_DIFF_NF = "DepartmentPartDiffNf";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(
                new CacheConfiguration("partitioned" + NUM_OF_PARTITIONS + "*")
                    .setAffinity(new RendezvousAffinityFunction(false, NUM_OF_PARTITIONS)),
                new CacheConfiguration("partitioned" + NUM_OF_PARTITIONS + "_DIFF*")
                    .setAffinity(new RendezvousAffinityFunction(false, NUM_OF_PARTITIONS + 1)),
                new CacheConfiguration("partitioned" + NUM_OF_PARTITIONS + "_DIFF_NF*")
                    .setAffinity(new RendezvousAffinityFunction(false, NUM_OF_PARTITIONS))
                        .setNodeFilter(clusterNode -> true),
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

        createDepartmentTable(DEP_PART_TAB_DIFF, "template=partitioned" + NUM_OF_PARTITIONS + "_DIFF");

        fillDepartmentTable(DEP_PART_TAB_DIFF);

        createDepartmentTable(DEP_PART_TAB_DIFF_NF, "template=partitioned" + NUM_OF_PARTITIONS + "_DIFF_NF");

        fillDepartmentTable(DEP_PART_TAB_DIFF_NF);
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

    /**
     * Check LEFT JOIN with collocated data of replicated and partitioned tables with different affinity.
     * This test relies on having the same number of partitions in replicated and partitioned caches
     */
    @Test
    public void testLeftJoinReplicatedPartitionedDiffPartitionsErr() {
        GridTestUtils.assertThrows(log, () -> checkLeftJoinEmployeeDepartment(DEP_PART_TAB_DIFF), IgniteException.class,
                "only with the same partitions number configuration");
    }

    /**
     * Check RIGHT JOIN with collocated data of partitioned and replicated tables with different affinity.
     */
    @Test
    public void testRightJoinPartitionedReplicatedDiffPartitionsErr() {
        GridTestUtils.assertThrows(log, () -> checkRightJoinDepartmentEmployee(DEP_PART_TAB_DIFF), IgniteException.class,
                "only with the same partitions number configuration");
    }

    /**
     * Check LEFT JOIN with collocated data of replicated and partitioned tables with different node filter.
     * This test relies on having the same number of partitions in replicated and partitioned caches
     */
    @Test
    public void testLeftJoinReplicatedPartitionedDiffNodeFilterErr() {
        GridTestUtils.assertThrows(log, () -> checkLeftJoinEmployeeDepartment(DEP_PART_TAB_DIFF_NF), IgniteException.class,
                "due to different node filters configuration");
    }

    /**
     * Check RIGHT JOIN with collocated data of partitioned and replicated tables with different node filter.
     */
    @Test
    public void testRightJoinPartitionedReplicatedDiffNodeFilterErr() {
        GridTestUtils.assertThrows(log, () -> checkRightJoinDepartmentEmployee(DEP_PART_TAB_DIFF_NF), IgniteException.class,
                "due to different node filters configuration");
    }
}
