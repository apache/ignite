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

package org.apache.ignite.internal.processors.cache.persistence.db.checkpoint;

import java.io.File;
import java.io.IOException;
import java.nio.file.OpenOption;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.persistence.file.AsyncFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DEFAULT_DISK_PAGE_COMPRESSION;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 *
 */
public class CheckpointFailBeforeWriteMarkTest extends GridCommonAbstractTest {
    /** */
    private InterceptorIOFactory interceptorIOFactory = new InterceptorIOFactory();

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        DataStorageConfiguration storageCfg = new DataStorageConfiguration();

        //By some reasons in the compression case it is required more memory for reproducing.
        boolean isCompression = System.getProperty(IGNITE_DEFAULT_DISK_PAGE_COMPRESSION) != null;

        storageCfg.setCheckpointThreads(2)
            .setFileIOFactory(interceptorIOFactory)
            .setWalSegmentSize((isCompression ? 20 : 5) * 1024 * 1024)
            .setWalSegments(3);

        storageCfg.getDefaultDataRegionConfiguration()
            .setPersistenceEnabled(true)
            .setMaxSize((isCompression ? 70 : 10) * 1024 * 1024);

        cfg.setDataStorageConfiguration(storageCfg)
            .setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setAffinity(new RendezvousAffinityFunction(false, 16)));

        cfg.setFailureHandler(new StopNodeFailureHandler());

        return cfg;
    }

    /**
     * Test IO factory which given opportunity to throw IO exception by custom predicate.
     */
    private static class InterceptorIOFactory extends AsyncFileIOFactory {
        /** */
        private static final Predicate<File> DUMMY_PREDICATE = (f) -> false;

        /** Time to wait before exception would be thrown. It is giving time to page replacer to work. */
        private static final long DELAY_TIME = 1000;

        /** Predicate which is a trigger of throwing an exception. */
        transient volatile Predicate<File> failPredicate = DUMMY_PREDICATE;

        /** {@inheritDoc} */
        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            if (file.getName().contains("START.bin"))
                sleep();

            if (failPredicate.test(file)) {
                failPredicate = DUMMY_PREDICATE;

                throw new IOException("Triggered test exception");
            }

            return super.create(file, modes);
        }

        /** **/
        private void sleep() {
            try {
                Thread.sleep(DELAY_TIME);
            }
            catch (InterruptedException ignore) {
            }
        }

        /**
         * Triggering exception by custom predicate.
         *
         * @param failPredicate Predicate for exception.
         */
        public void triggerIOException(Predicate<File> failPredicate) {
            this.failPredicate = failPredicate;
        }
    }

    /**
     * @throws Exception if fail.
     */
    @Test
    public void testCheckpointFailBeforeMarkEntityWrite() throws Exception {
        //given: one node with persistence.
        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        //It is necessary to understanding when page replacement would be started.
        PageMemory pageMemory = ignite0.context().cache().context().database().dataRegion("default").pageMemory();

        //when: Load a lot of data to cluster.
        AtomicInteger lastKey = new AtomicInteger();
        GridTestUtils.runMultiThreadedAsync(() -> {
            IgniteCache<Integer, Object> cache2 = ignite(0).cache(DEFAULT_CACHE_NAME);

            //Should stopped putting data when node is fail.
            for (int i = 0; i < Integer.MAX_VALUE; i++) {
                cache2.put(i, i);

                lastKey.set(i);

                if (i % 1000 == 0)
                    log.info("WRITE : " + i);
            }
        }, 3, "LOAD-DATA");

        //and: Page replacement was started.
        assertTrue(waitForCondition(() -> (int)U.field(pageMemory, "pageReplacementWarned") > 0, 60_000));

        //and: Node was failed during checkpoint after write lock was released and before checkpoint marker was stored to disk.
        interceptorIOFactory.triggerIOException((file) -> file.getName().contains("START.bin"));

        log.info("KILL NODE await to stop");

        assertTrue(waitForCondition(() -> G.allGrids().isEmpty(), 20_000));

        //then: Data recovery after node start should be successful.
        ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        IgniteCache<Integer, Object> cache = ignite(0).cache(DEFAULT_CACHE_NAME);

        //WAL mode is 'default' so it is allowable to lost some last data(ex. last 100).
        for (int i = 0; i < lastKey.get() - 100; i++)
            assertNotNull(cache.get(i));
    }
}
