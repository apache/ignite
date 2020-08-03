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

package org.apache.ignite.internal.processors.cache.persistence;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 */
public class IgnitePdsRestartAfterFailedToWriteMetaPageTest extends GridCommonAbstractTest implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(32L * 1024 * 1024)
                    .setPersistenceEnabled(true))
            .setWalMode(WALMode.FSYNC)
            .setCheckpointFrequency(500L);

        memCfg.setFileIOFactory(new MyIOFactory());

        cfg.setDataStorageConfiguration(memCfg);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(DEFAULT_CACHE_NAME);
        ccfg.setCacheMode(CacheMode.REPLICATED);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
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
    public void test() throws Exception {
        IgniteEx grid = startGrid(0);

        startGrid(1);

        grid.cluster().active(true);

        AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(() -> {
            IgniteCache<Integer, Integer> cache = grid.cache(DEFAULT_CACHE_NAME);

            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            while (!stop.get())
                cache.put(rnd.nextInt(100_000), rnd.nextInt());
        }, 12, "cache-insert-thread");

        try {
            Thread.sleep(1_000);

            stopGrid(1);

            Thread.sleep(5_000);

            failNextCheckpoint = true;

            GridTestUtils.assertThrows(log, () -> startGrid(1), Exception.class, "");

            assertTrue(GridTestUtils.waitForCondition(() -> grid.cluster().nodes().size() == 1, 10_000));

            failNextCheckpoint = false;

            startGrid(1);
        }
        finally {
            stop.set(true);

            try {
                fut.get();
            }
            catch (Exception ignore) {
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return new StopNodeFailureHandler();
    }

    /** */
    private volatile boolean failNextCheckpoint;

    /** */
    private class MyIOFactory implements FileIOFactory {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** */
        private AtomicInteger cpCnt = new AtomicInteger();

        /** {@inheritDoc} */
        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            FileIO delegate = new RandomAccessFileIOFactory().create(file, modes);

            String nodeName = IgnitePdsRestartAfterFailedToWriteMetaPageTest.class.getSimpleName() + "1";

            if (file.getAbsolutePath().contains(nodeName)) {
                if (failNextCheckpoint && file.getName().contains("-START.bin"))
                    cpCnt.incrementAndGet();
                else if (file.getName().contains("part-") && !file.getAbsolutePath().contains("metastorage")) {
                    return new FileIODecorator(delegate) {
                        @Override public int write(ByteBuffer srcBuf) throws IOException {
                            maybeThrowException();

                            return super.write(srcBuf);
                        }

                        @Override public int write(ByteBuffer srcBuf, long position) throws IOException {
                            maybeThrowException();

                            return super.write(srcBuf, position);
                        }

                        @Override public int write(byte[] buf, int off, int len) throws IOException {
                            maybeThrowException();

                            return super.write(buf, off, len);
                        }

                        private void maybeThrowException() throws IOException {
                            if (failNextCheckpoint && cpCnt.get() > 0)
                                throw new IOException("Checkpoint failed.");
                        }
                    };
                }
            }

            return delegate;
        }
    }
}
