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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class IgniteShutdownOnSupplyMessageFailureTest extends GridCommonAbstractTest {
    /** Rebalance cache name. */
    private static final String TEST_REBALANCE_CACHE = "b13813zk";

    /** Wal history size. */
    private static final int WAL_HISTORY_SIZE = 30;

    /** Node name with test file factory. */
    private static final int NODE_NAME_WITH_TEST_FILE_FACTORY = 0;

    /** Wait on supply message failure. */
    private static final CountDownLatch WAIT_ON_SUPPLY_MESSAGE_FAILURE = new CountDownLatch(1);

    /** */
    private AtomicBoolean canFailFirstNode = new AtomicBoolean();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setConsistentId(name);

        DataStorageConfiguration conf = new DataStorageConfiguration()
            .setWalHistorySize(WAL_HISTORY_SIZE)
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(DataStorageConfiguration.DFLT_DATA_REGION_INITIAL_SIZE)
                    .setPersistenceEnabled(true)
            )
            .setWalMode(WALMode.FSYNC)
            .setCheckpointFrequency(500);

        if (name.equals(getTestIgniteInstanceName(NODE_NAME_WITH_TEST_FILE_FACTORY))) {
            conf.setFileIOFactory(new FailingFileIOFactory(canFailFirstNode));

            cfg.setFailureHandler(new TestFailureHandler());
        }
        else
            cfg.setFailureHandler(new StopNodeFailureHandler());

        cfg.setDataStorageConfiguration(conf);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        System.setProperty(IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD, "0");

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        System.clearProperty(IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD);

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Checks that we shutdown after a throwable into handleDemandMessage.
     */
    @Test
    public void testShutdownOnSupplyMessageFailure() throws Exception {
        IgniteEx ig = startGrid(0);
        IgniteEx awayNode = startGrid(1);

        ig.cluster().state(ClusterState.ACTIVE);

        createCache(ig, TEST_REBALANCE_CACHE);

        populateCache(ig, TEST_REBALANCE_CACHE, 0, 3_000);

        stopGrid(1);

        populateCache(ig, TEST_REBALANCE_CACHE, 3_000, 6_000);

        canFailFirstNode.set(true);

        startGrid(1);

        WAIT_ON_SUPPLY_MESSAGE_FAILURE.await();

        assertTrue(GridTestUtils.waitForCondition(() -> grid(1).context().discovery().aliveServerNodes().size() == 1,
            getTestTimeout()));

        assertFalse(awayNode.context().discovery().alive(ig.context().localNodeId())); // Only second node is alive
    }

    /**
     * @param ig Ig.
     * @param cacheName Cache name.
     */
    private void createCache(IgniteEx ig, String cacheName) {
        ig.getOrCreateCache(new CacheConfiguration<>(cacheName)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setAffinity(new RendezvousAffinityFunction(false, 1))
            .setBackups(1)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setRebalanceBatchSize(100));
    }

    /**
     * @param ig Ig.
     * @param cacheName Cache name.
     * @param startKey Start key range.
     * @param cnt Count.
     */
    private void populateCache(IgniteEx ig, String cacheName, int startKey, int cnt) throws IgniteCheckedException {
        try (IgniteDataStreamer<Object, Object> streamer = ig.dataStreamer(cacheName)) {
            for (int i = startKey; i < startKey + cnt; i++)
                streamer.addData(i, new byte[5 * 1000]);
        }

        GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)ig.context().cache().context().database();

        dbMgr.waitForCheckpoint("test");
    }

    /**
     * Test failure handler.
     */
    private static class TestFailureHandler extends StopNodeFailureHandler {
        /** {@inheritDoc} */
        @Override public boolean handle(Ignite ignite, FailureContext failureCtx) {
            Throwable err = X.cause(failureCtx.error(), IOException.class);

            if (err != null && err.getMessage() != null && err.getMessage().contains("Test crash"))
                WAIT_ON_SUPPLY_MESSAGE_FAILURE.countDown();

            return super.handle(ignite, failureCtx);
        }
    }

    /**
     * Create File I/O which fails after second attempt to write to File
     */
    private static class FailingFileIOFactory implements FileIOFactory {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Delegate factory. */
        private AtomicBoolean fail;

        /** */
        private final FileIOFactory delegateFactory = new RandomAccessFileIOFactory();

        /** */
        FailingFileIOFactory(AtomicBoolean fail) {
            this.fail = fail;
        }

        /** {@inheritDoc} */
        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            final FileIO delegate = delegateFactory.create(file, modes);

            return new FileIODecorator(delegate) {

                @Override public int read(ByteBuffer destBuf, long position) throws IOException {
                    if (fail != null && fail.get())
                        throw new IOException("Test crash.");

                    return super.read(destBuf, position);
                }

                @Override public int read(ByteBuffer destBuf) throws IOException {
                    if (fail != null && fail.get())
                        throw new IOException("Test crash.");

                    return super.read(destBuf);
                }

                @Override public int read(byte[] buf, int off, int len) throws IOException {
                    if (fail != null && fail.get())
                        throw new IOException("Test crash.");

                    return super.read(buf, off, len);
                }
            };
        }
    }
}
