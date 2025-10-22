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

package org.apache.ignite.cache.affinity.rendezvous;

import java.util.Collection;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class MdcAffinityBackupFilterSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String DC_0_ID = "DC_0";

    /** */
    private static final String DC_1_ID = "DC_1";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration optimize(IgniteConfiguration cfg) throws IgniteCheckedException {
        return super.optimize(cfg).setIncludeProperties((String[])null);
    }

    /**
     * Verifies that partition copies are assigned evenly to all data centers.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testBasicDistribution() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, DC_0_ID);
        startGrids(2);

        System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, DC_1_ID);
        startGrid(2);
        IgniteEx srv = startGrid(3);

        CacheConfiguration ccfg = defaultCacheConfiguration()
            .setBackups(3)
            .setAffinity(
                new RendezvousAffinityFunction(false, 4)
                    .setAffinityBackupFilter(new MdcAffinityBackupFilter(2, 3)));

        IgniteCache cache = srv.getOrCreateCache(ccfg);

        checkPartitions(srv, cache, 3);
    }

    /** */
    private void checkPartitions(IgniteEx srv, IgniteCache cache, int backups) throws IgniteCheckedException {) {
        int partCnt = cacheConfiguration(srv.configuration(), cache.getName()).getAffinity().partitions();
        Affinity aff = affinity(cache);

        for (int i = 0; i < partCnt; i++) {
            Collection<ClusterNode> nodes = aff.mapKeyToPrimaryAndBackups(i);
        }
    }
}
