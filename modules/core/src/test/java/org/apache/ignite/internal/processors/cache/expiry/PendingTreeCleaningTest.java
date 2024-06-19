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

package org.apache.ignite.internal.processors.cache.expiry;

import java.util.Collection;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 *
 */
@RunWith(Parameterized.class)
public class PendingTreeCleaningTest extends GridCommonAbstractTest {
    /** */
    @Parameterized.Parameter
    public boolean pds;

    /** */
    @Parameterized.Parameters(name = "pds={0}")
    public static Collection<?> parameters() {
        return F.asList(false, true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(pds));

        cfg.setDataStorageConfiguration(dsCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPendingTreeCleaningOnCacheDestroy() throws Exception {
        IgniteEx ignite = startGrid();

        ignite.cluster().state(ClusterState.ACTIVE);

        String cache1 = "cache1";
        String cache2 = "cache2";
        String grp = "grp";

        ignite.getOrCreateCache(new CacheConfiguration<>(cache1).setGroupName(grp)
            .setExpiryPolicyFactory(FactoryBuilder.factoryOf(new CreatedExpiryPolicy(Duration.ONE_DAY))));

        ignite.getOrCreateCache(new CacheConfiguration<>(cache2).setGroupName(grp)
            .setExpiryPolicyFactory(FactoryBuilder.factoryOf(new CreatedExpiryPolicy(Duration.ONE_DAY))));

        int cnt = 10_000;

        try (IgniteDataStreamer<Object, Object> ds = ignite.dataStreamer(cache1)) {
            for (int i = 0; i < cnt; i++)
                ds.addData(i, i);
        }

        try (IgniteDataStreamer<Object, Object> ds = ignite.dataStreamer(cache2)) {
            for (int i = 0; i < cnt; i++)
                ds.addData(i, i);
        }

        CacheGroupContext gctx = ignite.context().cache().cache(cache1).context().group();

        assertEquals(cnt * 2, gctx.offheap().expiredSize());

        ignite.destroyCache(cache2);

        assertEquals(cnt, gctx.offheap().expiredSize());
    }
}
