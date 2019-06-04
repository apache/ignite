/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.cache.persistence;

import java.io.File;
import java.io.Serializable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.CallbackExecutorLogListener;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.MessageOrderLogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class RebuildIndexLogMessageTest extends GridCommonAbstractTest implements Serializable {
    /** */
    private static final String CACHE_NAME_A = "testCacheA";

    /** */
    private static final String CACHE_NAME_B = "testCacheB";

    /** */
    private final CountDownLatch checkLatch = new CountDownLatch(1);

    /** */
    private final LogListener logLsnr = new MessageOrderLogListener(
        new MessageOrderLogListener.MessageGroup(true)
            //message group for all caches
            .add(
                new MessageOrderLogListener.MessageGroup(false)
                    .add(
                        //message group for testCacheA
                        new MessageOrderLogListener.MessageGroup(true)
                            .add("Started indexes rebuilding for cache \\[name=testCacheA.*")
                            .add("((Finished indexes rebuilding)|(Failed to rebuild indexes)) for cache " +
                                "\\[name=testCacheA.*")
                    )
                    .add(
                        //message group for testCacheB
                        new MessageOrderLogListener.MessageGroup(true)
                            .add("Started indexes rebuilding for cache \\[name=testCacheB.*")
                            .add("((Finished indexes rebuilding)|(Failed to rebuild indexes)) for cache " +
                                "\\[name=testCacheB.*")
                    )
            )
            //message that appears after rebuilding is completed for all caches
            .add("Indexes rebuilding completed for all caches.")
    );

    /** */
    private final LogListener latchLsnr =
        new CallbackExecutorLogListener("Indexes rebuilding completed for all caches.", checkLatch::countDown);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        ListeningTestLogger testLog =
            new ListeningTestLogger(false, super.getConfiguration(igniteInstanceName).getGridLogger());

        testLog.registerListener(logLsnr);
        testLog.registerListener(latchLsnr);

        return super.getConfiguration(igniteInstanceName)
            .setGridLogger(testLog)
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration()
                            .setPersistenceEnabled(true)
                    )
            )
            .setCacheConfiguration(
                new CacheConfiguration<>()
                    .setName(CACHE_NAME_A)
                    .setBackups(0)
                    .setIndexedTypes(Integer.class, Person.class)
                    .setAffinity(new RendezvousAffinityFunction(false, 8)),
                new CacheConfiguration<>()
                    .setName(CACHE_NAME_B)
                    .setBackups(0)
                    .setIndexedTypes(Integer.class, Person.class)
                    .setAffinity(new RendezvousAffinityFunction(false, 8))
            );
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /** */
    @Test
    public void testRebuildIndexLogMessage() throws Exception {
        IgniteEx ignite = startGrids(1);

        String gridName = ignite.name();

        ignite.cluster().active(true);

        IgniteCache<Integer, Person> cacheA = ignite.getOrCreateCache(CACHE_NAME_A);
        IgniteCache<Integer, Person> cacheB = ignite.getOrCreateCache(CACHE_NAME_B);

        IgniteInternalCache<Integer, Person> cacheAEx = ignite.cachex(CACHE_NAME_A);
        IgniteInternalCache<Integer, Person> cacheBEx = ignite.cachex(CACHE_NAME_B);

        for (int i = 0; i < 100; i++)
            cacheA.put(i, new Person(i, i));

        for (int i = 0; i < 100; i++)
            cacheB.put(i, new Person(i, i));

        forceCheckpoint();

        File cacheAWorkDir = ((FilePageStoreManager)cacheAEx.context().shared().pageStore())
            .cacheWorkDir(cacheAEx.configuration());
        File cacheBWorkDir = ((FilePageStoreManager)cacheBEx.context().shared().pageStore())
            .cacheWorkDir(cacheBEx.configuration());

        File idxPathA = cacheAWorkDir.toPath().resolve("index.bin").toFile();
        File idxPathB = cacheBWorkDir.toPath().resolve("index.bin").toFile();

        stopAllGrids();

        assertTrue(U.delete(idxPathA));
        assertTrue(U.delete(idxPathB));

        ignite = startGrid(getConfiguration(gridName));

        ignite.cluster().active(true);

        cacheA = ignite.getOrCreateCache(CACHE_NAME_A);
        cacheB = ignite.getOrCreateCache(CACHE_NAME_B);

        cacheA.put(1000, new Person(1000, 1));
        cacheB.put(1000, new Person(1000, 1));

        checkLatch.await(60, TimeUnit.SECONDS);

        if (checkLatch.getCount() > 0)
            throw new TimeoutException("Test timed out: cannot detect log message about completion of indexes rebuilding");

        assertTrue(logLsnr.check());
    }

    /** */
    private static class Person implements Serializable {
        /** */
        @QuerySqlField(index = true)
        private int id;

        /** */
        @QuerySqlField(index = true)
        private int age;

        /**
         * @param id Id.
         * @param age Age.
         */
        Person(int id, int age) {
            this.id = id;
            this.age = age;
        }

        /**
         * @return Age/
         */
        public int age() {
            return age;
        }
    }
}
