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

package org.apache.ignite.internal.processors.cache.persistence.db;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.commandline.cache.argument.FindAndDeleteGarbageArg;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.cache.VisorFindAndDeleteGarbageInPersistenceJobResult;
import org.apache.ignite.internal.visor.cache.VisorFindAndDeleteGarbageInPersistenceTask;
import org.apache.ignite.internal.visor.cache.VisorFindAndDeleteGarbageInPersistenceTaskArg;
import org.apache.ignite.internal.visor.cache.VisorFindAndDeleteGarbageInPersistenceTaskResult;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.IGNITE_PDS_SKIP_CHECKPOINT_ON_NODE_STOP;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/**
 * Testing corner cases in cache group functionality: -stopping cache in shared group and immediate node leaving;
 * -starting cache in shared group with the same name as destroyed one; -etc.
 */
@WithSystemProperty(key = IGNITE_PDS_SKIP_CHECKPOINT_ON_NODE_STOP, value = "true")
@SuppressWarnings({"unchecked", "ThrowableNotThrown"})
public class IgniteCacheGroupsWithRestartsTest extends GridCommonAbstractTest {
    /** Group name. */
    public static final String GROUP = "group";

    /**
     *
     */
    private volatile boolean startExtraStaticCache;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration configuration = super.getConfiguration(gridName);

        configuration.setConsistentId(gridName);

        configuration.setConnectorConfiguration(new ConnectorConfiguration());

        DataStorageConfiguration cfg = new DataStorageConfiguration();

        cfg.setDefaultDataRegionConfiguration(new DataRegionConfiguration()
            .setPersistenceEnabled(true)
            .setMaxSize(256 * 1024 * 1024));

        configuration.setDataStorageConfiguration(cfg);

        if (startExtraStaticCache)
            configuration.setCacheConfiguration(getCacheConfiguration(3));

        return configuration;
    }

    /**
     * @param i Cache index number.
     * @return Cache configuration with the given number in name.
     */
    private CacheConfiguration<Object, Object> getCacheConfiguration(int i) {
        CacheConfiguration ccfg = new CacheConfiguration();

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        fields.put("updateDate", "java.lang.Date");
        fields.put("amount", "java.lang.Long");
        fields.put("name", "java.lang.String");

        Set<QueryIndex> indices = Collections.singleton(new QueryIndex("name", QueryIndexType.SORTED));

        ccfg.setName(getCacheName(i))
            .setGroupName("group")
            .setQueryEntities(Collections.singletonList(
                new QueryEntity(Long.class, Account.class)
                    .setFields(fields)
                    .setIndexes(indices)
            ))
            .setAffinity(new RendezvousAffinityFunction(false, 64));

        return ccfg;
    }

    /**
     * @param i Index.
     * @return Generated cache name for index.
     */
    private String getCacheName(int i) {
        return "cache-" + i;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        afterTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-8717")
    @Test
    public void testNodeRestartRightAfterCacheStop() throws Exception {
        IgniteEx ex = startGrids(3);

        prepareCachesAndData(ex);

        ex.destroyCache(getCacheName(0));

        assertNull(ex.cachex(getCacheName(0)));

        stopGrid(2, true);

        startGrid(2);

        assertNull(ex.cachex(getCacheName(0)));

        IgniteCache<Object, Object> cache = ex.createCache(getCacheConfiguration(0));

        awaitPartitionMapExchange();

        assertEquals(0, cache.size());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNodeRestartBetweenCacheStop() throws Exception {
        IgniteEx ex = startGrids(3);

        prepareCachesAndData(ex);

        stopGrid(2, true);

        ex.destroyCache(getCacheName(0));

        assertNull(ex.cachex(getCacheName(0)));

        try {
            startGrid(2);

            fail();
        }
        catch (Exception e) {
            List<Throwable> list = X.getThrowableList(e);

            assertTrue(list.stream().
                anyMatch(x -> x.getMessage().
                    contains("Joining node has caches with data which are not presented on cluster")));
        }

        removeCacheDir(getTestIgniteInstanceName(2), "cacheGroup-group");

        IgniteEx node2 = startGrid(2);

        assertEquals(3, node2.cluster().nodes().size());
    }

    /**
     * @param instanceName Instance name.
     * @param cacheGroup Cache group.
     */
    private void removeCacheDir(String instanceName, String cacheGroup) throws IgniteCheckedException {
        String dn2DirName = instanceName.replace(".", "_");

        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(),
            DFLT_STORE_DIR + "/" + dn2DirName + "/" + cacheGroup, true));
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-12072")
    @Test
    public void testNodeRestartWithNewStaticallyConfiguredCache() throws Exception {
        IgniteEx ex = startGrids(3);

        prepareCachesAndData(ex);

        stopGrid(2, true);

        assertNull(ex.cachex(getCacheName(3)));

        startExtraStaticCache = true;

        IgniteEx node2;
        try {
            node2 = startGrid(2);
        }
        finally {
            startExtraStaticCache = false;
        }

        assertNotNull(ex.cachex(getCacheName(3)));
        assertNotNull(node2.cachex(getCacheName(3)));

        IgniteCache<Object, Object> cache = ex.cache(getCacheName(3));

        assertEquals(0, cache.size());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCleaningGarbageAfterCacheDestroyedAndNodeStop() throws Exception {
        testFindAndDeleteGarbage(this::executeTask);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCleaningGarbageAfterCacheDestroyedAndNodeStop_ControlConsoleUtil() throws Exception {
        testFindAndDeleteGarbage(this::executeTaskViaControlConsoleUtil);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNodeRestartWith3rdPartyCacheStoreAndPersistenceEnabled() throws Exception {
        IgniteEx crd = startGrid(0);

        crd.cluster().state(ACTIVE);

        String cacheName = "test-cache-3rd-party-write-behind-and-ignite-persistence";
        CacheConfiguration ccfg = new CacheConfiguration(cacheName)
            .setWriteBehindEnabled(true)
            .setWriteThrough(true)
            .setReadThrough(true)
            .setCacheStoreFactory(new StoreFactory());

        IgniteCache cache = crd.getOrCreateCache(ccfg);

        cache.put(12, 42);

        stopGrid(0);

        crd = startGrid(0);

        crd.cluster().state(ACTIVE);

        cache = crd.cache(cacheName);

        assertEquals("Cache was not properly restored or required key is lost.", 42, cache.get(12));
    }

    /**
     * @param doFindAndRemove Do find and remove.
     */
    private void testFindAndDeleteGarbage(
        BiFunction<IgniteEx, Boolean, VisorFindAndDeleteGarbageInPersistenceTaskResult> doFindAndRemove
    ) throws Exception {
        IgniteEx ignite = startGrids(3);

        prepareCachesAndData(ignite);

        ignite.destroyCache(getCacheName(0));

        assertNull(ignite.cachex(getCacheName(0)));

        Thread.sleep(5_000); // waiting for cache.dat deletion

        stopGrid(2, true);

        IgniteEx ex1 = startGrid(2);

        assertNull(ignite.cachex(getCacheName(0)));

        ignite.resetLostPartitions(Arrays.asList(getCacheName(0), getCacheName(1), getCacheName(2)));

        awaitPartitionMapExchange();

        VisorFindAndDeleteGarbageInPersistenceTaskResult taskResult = doFindAndRemove.apply(ex1, false);

        VisorFindAndDeleteGarbageInPersistenceJobResult result = taskResult.result().get(ex1.localNode().id());

        Assert.assertTrue(result.hasGarbage());

        Assert.assertTrue(result.checkResult()
            .get(CU.cacheId("group"))
            .get(CU.cacheId(getCacheName(0))) > 0);

        //removing garbage
        result = doFindAndRemove.apply(ex1, true).result().get(ex1.localNode().id());

        Assert.assertTrue(result.hasGarbage());

        result = doFindAndRemove.apply(ex1, false).result().get(ex1.localNode().id());

        Assert.assertFalse(result.hasGarbage());
    }

    /**
     * @param ignite Ignite to execute task on.
     * @param deleteFoundGarbage If clearing mode should be used.
     * @return Result of task run.
     */
    private VisorFindAndDeleteGarbageInPersistenceTaskResult executeTask(
        IgniteEx ignite,
        boolean deleteFoundGarbage
    ) {
        VisorFindAndDeleteGarbageInPersistenceTaskArg group = new VisorFindAndDeleteGarbageInPersistenceTaskArg(
            Collections.singleton(GROUP), deleteFoundGarbage, null);

        UUID id = ignite.localNode().id();

        VisorTaskArgument arg = new VisorTaskArgument(id, group, true);

        VisorFindAndDeleteGarbageInPersistenceTaskResult result =
            ignite.compute().execute(VisorFindAndDeleteGarbageInPersistenceTask.class, arg);

        return result;
    }

    /**
     * @param ignite Ignite to execute task on.
     * @param delFoundGarbage If clearing mode should be used.
     * @return Result of task run.
     */
    private VisorFindAndDeleteGarbageInPersistenceTaskResult executeTaskViaControlConsoleUtil(
        IgniteEx ignite,
        boolean delFoundGarbage
    ) {
        CommandHandler hnd = new CommandHandler();

        List<String> args = new ArrayList<>(Arrays.asList("--yes", "--port", "11212", "--cache", "find_garbage",
            ignite.localNode().id().toString()));

        if (delFoundGarbage)
            args.add(FindAndDeleteGarbageArg.DELETE.argName());

        hnd.execute(args);

        return hnd.getLastOperationResult();
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

                cache.put((long)i, new Account(i));
            }
        }
    }

    /**
     *
     */
    static class Account {
        /**
         *
         */
        private final int val;

        /**
         * @param val Value.
         */
        public Account(int val) {
            this.val = val;
        }

        /**
         * @return Value.
         */
        public int value() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Account.class, this);
        }
    }

    /**
     * Test store factory.
     */
    private static class StoreFactory implements Factory<CacheStore> {
        /** {@inheritDoc} */
        @Override public CacheStore create() {
            return new TestStore();
        }
    }

    /**
     * Test store.
     */
    private static class TestStore extends CacheStoreAdapter {
        /** {@inheritDoc} */
        @Override public Object load(Object key) throws CacheLoaderException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry entry) throws CacheWriterException {
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
        }
    }
}
