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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cache.affinity.fair.*;
import org.apache.ignite.cache.affinity.rendezvous.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.testframework.junits.common.*;

/**
 * Tests affinity assignment for different affinity types.
 */
public class IgniteClientAffinityAssignmentSelfTest extends GridCommonAbstractTest {
    /** */
    public static final int PARTS = 256;

    /** */
    private boolean client;

    /** */
    private boolean cache;

    /** */
    private int aff;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (cache && !client) {
            CacheConfiguration ccfg = new CacheConfiguration();

            ccfg.setCacheMode(CacheMode.PARTITIONED);
            ccfg.setBackups(1);
            ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

            ccfg.setNearConfiguration(null);

            if (aff == 0)
                ccfg.setAffinity(new CacheRendezvousAffinityFunction(false, PARTS));
            else
                ccfg.setAffinity(new CachePartitionFairAffinity(PARTS));

            cfg.setCacheConfiguration(ccfg);
        }

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testRendezvousAssignment() throws Exception {
        aff = 0;

        checkAffinityFunction();
    }

    /**
     * @throws Exception If failed.
     */
    public void testFairAssignment() throws Exception {
        aff = 1;

        checkAffinityFunction();
    }

    /**
     * @throws Exception If failed.
     */
    private void checkAffinityFunction() throws Exception {
        cache = true;

        startGrids(3);

        try {
            checkAffinity();

            client = true;

            startGrid(3);

            checkAffinity();

            startGrid(4);

            checkAffinity();

            cache = false;

            startGrid(5);

            checkAffinity();

            stopGrid(5);

            checkAffinity();

            stopGrid(4);

            checkAffinity();

            stopGrid(3);

            checkAffinity();
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void checkAffinity() throws Exception {
        CacheAffinity<Object> aff = ((IgniteKernal)grid(0)).getCache(null).affinity();

        for (Ignite grid : Ignition.allGrids()) {
            try {
                if (grid.cluster().localNode().id().equals(grid(0).localNode().id()))
                    continue;

                CacheAffinity<Object> checkAff = ((IgniteKernal)grid).getCache(null).affinity();

                for (int p = 0; p < PARTS; p++)
                    assertEquals(aff.mapPartitionToPrimaryAndBackups(p), checkAff.mapPartitionToPrimaryAndBackups(p));
            }
            catch (IllegalArgumentException ignored) {
                // Skip the node without cache.
            }
        }
    }
}
