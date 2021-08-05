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

package com.sbt.sbergrid.extras;

import java.util.Collection;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionBackupFilterAbstractSelfTest;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;

/**
 * Partitioned affinity test.
 */
public class ClusterNodeAttributeColocatedBackupFilterSelfTest extends AffinityFunctionBackupFilterAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected AffinityFunction affinityFunction() {
        RendezvousAffinityFunction aff = new RendezvousAffinityFunction(false);

        aff.setBackupFilter(backupFilter);

        return aff;
    }

    /** {@inheritDoc} */
    @Override protected AffinityFunction affinityFunctionWithAffinityBackupFilter(String attributeName) {
        RendezvousAffinityFunction aff = new RendezvousAffinityFunction(false);

        aff.setAffinityBackupFilter(new ClusterNodeAttributeColocatedBackupFilter(attributeName));

        return aff;
    }

    /** {@inheritDoc} */
    @Override protected void checkPartitionsWithAffinityBackupFilter() throws Exception {
        AffinityFunction aff = cacheConfiguration(grid(0).configuration(), DEFAULT_CACHE_NAME).getAffinity();

        int partCnt = aff.partitions();

        int iter = grid(0).cluster().nodes().size() / 4;

        IgniteCache<Object, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < partCnt; i++) {
            Collection<ClusterNode> nodes = affinity(cache).mapKeyToPrimaryAndBackups(i);

            Map<String, Integer> stat = getAttributeStatistic(nodes);

            if (stat.get(FIRST_NODE_GROUP) > 0) {
                assertEquals((Integer)Math.min(backups + 1, iter * 2), stat.get(FIRST_NODE_GROUP));
                assertEquals((Integer)0, stat.get("B"));
                assertEquals((Integer)0, stat.get("C"));
            }
            else if (stat.get("B") > 0) {
                assertEquals((Integer)0, stat.get(FIRST_NODE_GROUP));
                assertEquals((Integer)iter, stat.get("B"));
                assertEquals((Integer)0, stat.get("C"));
            }
            else if (stat.get("C") > 0) {
                assertEquals((Integer)0, stat.get(FIRST_NODE_GROUP));
                assertEquals((Integer)0, stat.get("B"));
                assertEquals((Integer)iter, stat.get("C"));
            }
            else
                fail("Unexpected partition assignment");
        }
    }

    /** {@inheritDoc} */
    @Override public void testPartitionDistributionWithAffinityBackupFilter() throws Exception {
        backups = 2;

        super.testPartitionDistributionWithAffinityBackupFilter();
    }
}
