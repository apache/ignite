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

package org.apache.ignite.internal.processors.cache.distributed;

import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.PartitionLossPolicy.IGNORE;
import static org.apache.ignite.cache.PartitionLossPolicy.READ_WRITE_SAFE;

/**
 * Checks that the other nodes learn the state of a lost partition from its new owner. A server leaves with the only
 * copies of some partitions. At the end of the exchange the new owners own them under the IGNORE policy or mark them
 * LOST under the safe policies. Only the owners write this state into their rows of the partition map.
 */
public class PartitionStateFromOwnerTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE = "cache";

    /** Partition loss policy of the cache. */
    private PartitionLossPolicy plc;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(new CacheConfiguration<Integer, Integer>(CACHE)
                .setBackups(0)
                .setPartitionLossPolicy(plc));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLostPartitionOwned() throws Exception {
        checkServerLeft(IGNORE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLostPartitionMarkedLost() throws Exception {
        checkServerLeft(READ_WRITE_SAFE);
    }

    /**
     * @param plc Partition loss policy.
     * @throws Exception If failed.
     */
    private void checkServerLeft(PartitionLossPolicy plc) throws Exception {
        this.plc = plc;

        startGrids(3);
        startClientGrid(3);

        // Evictions must end first: a node resends its map after its last eviction, and that map brings its new states.
        awaitPartitionMapExchange(true, true, null);

        stopGrid(2);

        awaitPartitionMapsMatchLocalStates(CACHE);
    }
}
