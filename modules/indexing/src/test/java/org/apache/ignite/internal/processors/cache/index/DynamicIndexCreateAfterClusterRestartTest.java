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

package org.apache.ignite.internal.processors.cache.index;

import java.util.concurrent.CountDownLatch;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.discovery.CustomEventListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.query.schema.message.SchemaProposeDiscoveryMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class DynamicIndexCreateAfterClusterRestartTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(
                new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration().setPersistenceEnabled(true)));

        cfg.setConsistentId(igniteInstanceName);
        cfg.setBuildIndexThreadPoolSize(1);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testNodeJoinOnCreateIndex() throws Exception {
        IgniteEx grid = startGrids(2);
        grid.cluster().state(ClusterState.ACTIVE);

        grid.getOrCreateCache(new CacheConfiguration<>("CACHE1").setSqlSchema("PUBLIC")
            .setIndexedTypes(Integer.class, Integer.class));
        grid.getOrCreateCache(new CacheConfiguration<>("CACHE2").setSqlSchema("PUBLIC")
            .setIndexedTypes(Integer.class, TestValue.class));

        stopAllGrids();

        startGrids(2);

        try (IgniteDataStreamer<Integer, TestValue> ds = grid(0).dataStreamer("CACHE2")) {
            for (int i = 0; i < 10_000; i++)
                ds.addData(i, new TestValue(i));
        }

        // Block index building.
        CountDownLatch idxBuild = new CountDownLatch(1);

        grid(1).context().pools().buildIndexExecutorService().execute(() -> {
            try {
                idxBuild.await();
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        // Wait for index create command on remote node.
        CountDownLatch idxCreateCmd = new CountDownLatch(1);

        grid(0).context().discovery().setCustomEventListener(SchemaProposeDiscoveryMessage.class,
            new CustomEventListener<SchemaProposeDiscoveryMessage>() {
                @Override public void onCustomEvent(
                    AffinityTopologyVersion topVer,
                    ClusterNode snd,
                    SchemaProposeDiscoveryMessage msg
                ) {
                    idxCreateCmd.countDown();
                }
            }
        );

        // Start dynamic index creation.
        IgniteInternalFuture fut = GridTestUtils.runAsync(() -> {
            grid(1).cache("CACHE2").query(new SqlFieldsQuery("CREATE INDEX ON TestValue(val)")).getAll();
        });

        idxCreateCmd.await();

        stopGrid(0, true);

        cleanPersistenceDir(getTestIgniteInstanceName(0));

        startGrid(0);

        // Resume index building.
        idxBuild.countDown();

        fut.get();
    }

    /** */
    private static class TestValue {
        /** */
        @QuerySqlField
        private final int val;

        /** */
        private TestValue(int val) {
            this.val = val;
        }
    }
}
