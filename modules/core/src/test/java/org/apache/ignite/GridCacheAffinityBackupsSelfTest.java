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

package org.apache.ignite;

import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cache.affinity.fair.*;
import org.apache.ignite.cache.affinity.rendezvous.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.*;

/**
 * Tests affinity function with different number of backups.
 */
public class GridCacheAffinityBackupsSelfTest extends GridCommonAbstractTest {
    /** Number of backups. */
    private int backups;

    /** Affinity function. */
    private AffinityFunction func;

    /** */
    private int nodesCnt = 5;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setBackups(backups);
        ccfg.setAffinity(func);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testRendezvousBackups() throws Exception {
        for (int i = 0; i < nodesCnt; i++)
            checkBackups(i, new RendezvousAffinityFunction());
    }

    /**
     * @throws Exception If failed.
     */
    public void testFairBackups() throws Exception {
        for (int i = 0; i < nodesCnt; i++)
            checkBackups(i, new FairAffinityFunction());
    }

    /**
     * @param backups Number of backups.
     * @param func Affinity function.
     * @throws Exception If failed.
     */
    private void checkBackups(int backups, AffinityFunction func) throws Exception {
        this.backups = backups;
        this.func = func;

        startGrids(nodesCnt);

        try {
            IgniteCache<Object, Object> cache = jcache(0);

            Collection<UUID> members = new HashSet<>();

            for (int i = 0; i < 10000; i++) {
                Collection<ClusterNode> nodes = affinity(cache).mapKeyToPrimaryAndBackups(i);

                assertEquals(backups + 1, nodes.size());

                for (ClusterNode n : nodes)
                    members.add(n.id());
            }

            assertEquals(nodesCnt, members.size());
        }
        finally {
            stopAllGrids();
        }
    }
}
