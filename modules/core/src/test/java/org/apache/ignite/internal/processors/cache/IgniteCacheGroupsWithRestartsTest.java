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

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopologyImpl;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.cache.VisorFindAndDeleteGarbargeInPersistenceJobResult;
import org.apache.ignite.internal.visor.cache.VisorFindAndDeleteGarbargeInPersistenceTask;
import org.apache.ignite.internal.visor.cache.VisorFindAndDeleteGarbargeInPersistenceTaskArg;
import org.apache.ignite.internal.visor.cache.VisorFindAndDeleteGarbargeInPersistenceTaskResult;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
@SuppressWarnings({"unchecked", "ThrowableNotThrown"})
public class IgniteCacheGroupsWithRestartsTest extends GridCommonAbstractTest {

    public static final String GROUP = "group";

    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration configuration = super.getConfiguration(gridName);

        DataStorageConfiguration cfg = new DataStorageConfiguration();

        cfg.setDefaultDataRegionConfiguration(new DataRegionConfiguration()
            .setPersistenceEnabled(true)
            .setMaxSize(256 * 1024 * 1024));

        configuration.setDataStorageConfiguration(cfg);

        return configuration;
    }

    private CacheConfiguration<Object, Object> getCacheConfiguration(int i) {
        return new CacheConfiguration<>(getCacheName(i))
            .setGroupName("group")
            .setAffinity(new RendezvousAffinityFunction(false, 64));
    }

    @NotNull private String getCacheName(int i) {
        return "cache-" + i;
    }

    @Override protected void beforeTest() throws Exception {
        afterTest();
    }

    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    @Test
    public void testNodeRestartRightAfterCacheStop() throws Exception {
        IgniteEx ex = startGrids(3);

        prepareCachesAndData(ex);

        ex.destroyCache(getCacheName(0));

        assertNull(ex.cachex(getCacheName(0)));

        IgniteProcessProxy.kill(grid(2).configuration().getIgniteInstanceName());

        startGrid(2);

        assertNull(ex.cachex(getCacheName(0)));

        IgniteCache<Object, Object> cache = ex.createCache(getCacheConfiguration(0));

        awaitPartitionMapExchange();

        assertEquals(0, cache.size());
    }

    @Test
    public void test2() throws Exception {
        IgniteEx ignite = startGrids(3);

        prepareCachesAndData(ignite);

        ignite.destroyCache(getCacheName(0));

        assertNull(ignite.cachex(getCacheName(0)));

        Thread.sleep(5_000); //waiting for cache.dat deletion

        IgniteProcessProxy.kill(grid(2).configuration().getIgniteInstanceName());

        IgniteProcessProxy ex1 = (IgniteProcessProxy)startGrid(2);

        assertNull(ignite.cachex(getCacheName(0)));

        awaitPartitionMapExchange();

        VisorFindAndDeleteGarbargeInPersistenceJobResult result = executeTask(ignite, ex1, false);

        Assert.assertTrue(result.hasGarbarge());

        Assert.assertTrue(result.checkResult()
                        .get(CU.cacheId("group"))
                        .get(CU.cacheId(getCacheName(0))) > 0);

        //removing garbage
        result = executeTask(ignite, ex1, true);

        Assert.assertTrue(result.hasGarbarge());

        result = executeTask(ignite, ex1, false);

        Assert.assertFalse(result.hasGarbarge());
    }

    private VisorFindAndDeleteGarbargeInPersistenceJobResult executeTask(
        IgniteEx ignite,
        IgniteProcessProxy ex1,
        boolean deleteFoundGarbarge
    ) {
        VisorFindAndDeleteGarbargeInPersistenceTaskArg group = new VisorFindAndDeleteGarbargeInPersistenceTaskArg(
            Collections.singleton(GROUP), deleteFoundGarbarge, null);

        UUID id = ex1.getId();

        VisorTaskArgument arg = new VisorTaskArgument(id, group, true);

        VisorFindAndDeleteGarbargeInPersistenceTaskResult execute =
            ignite.compute().execute(VisorFindAndDeleteGarbargeInPersistenceTask.class, arg);

        return execute.result().get(id);
    }

    /**
     * @param ignite Ignite instance.
     */
    private void prepareCachesAndData(IgniteEx ignite) {
        ignite.cluster().active(true);

        for (int j = 0; j < 3; j++) {
            for (int i = 0; i < 64 * 10; i++) {
                IgniteCache<Object, Object> cache = ignite.getOrCreateCache(getCacheConfiguration(j));

                byte[] val = new byte[ThreadLocalRandom.current().nextInt(8148)];

                Arrays.fill(val, (byte)i);

                cache.put(i, val);
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }
}
