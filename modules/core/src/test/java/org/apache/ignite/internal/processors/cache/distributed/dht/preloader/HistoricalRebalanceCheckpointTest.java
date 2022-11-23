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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyResultV2;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class HistoricalRebalanceCheckpointTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setWalMode(WALMode.LOG_ONLY)
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setMaxSize(50L * 1024 * 1024).setPersistenceEnabled(true)
            );

        cfg.setDataStorageConfiguration(dsCfg);

        cfg.getDataStorageConfiguration().setFileIOFactory(
            new BlockableFileIOFactory(cfg.getDataStorageConfiguration().getFileIOFactory()));

        cfg.getDataStorageConfiguration().setWalMode(WALMode.FSYNC); // Allows to use special IO at WAL as well.

        cfg.setFailureHandler(new StopNodeFailureHandler()); // Helps to kill nodes on stop with disabled IO.

        return cfg;
    }

    /**
     *
     */
    @Test
    public void testCountersOnCrashRecovery() throws Exception {
        int nodes = 3;
        int backupNodes = nodes - 1;

        IgniteEx ignite = startGrids(nodes);

        ignite.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache = ignite.createCache(new CacheConfiguration<>()
            .setAffinity(new RendezvousAffinityFunction(false, 1))
            .setBackups(backupNodes)
            .setName(DEFAULT_CACHE_NAME)
            .setAtomicityMode(TRANSACTIONAL)
            .setWriteSynchronizationMode(FULL_SYNC) // Allows to be sure that all messages are sent when put succeed.
            .setReadFromBackup(true)); // Allows to check values on backups.

        int updateCnt = 0;

        // Initial preloading.
        for (int i = 0; i < 2_000; i++)  // Enough to have historical rebalance when needed.
            cache.put(++updateCnt, updateCnt);

        // Trick to have historical rebalance on cluster recovery (decreases percent of updates in comparison to cache size).
        stopAllGrids();
        startGrids(nodes);

        Ignite prim = primaryNode(0L, DEFAULT_CACHE_NAME);
        List<Ignite> backups = backupNodes(0L, DEFAULT_CACHE_NAME);

        AtomicBoolean prepareBlock = new AtomicBoolean();
        AtomicBoolean finishBlock = new AtomicBoolean();

        AtomicReference<CountDownLatch> blockLatch = new AtomicReference<>();

        TestRecordingCommunicationSpi.spi(prim).blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode node, Message msg) {
                if ((msg instanceof GridDhtTxPrepareRequest && prepareBlock.get()) ||
                    (msg instanceof GridDhtTxFinishRequest && finishBlock.get())) {
                    CountDownLatch latch = blockLatch.get();

                    assertTrue(latch.getCount() > 0);

                    latch.countDown();

                    return true; // Generating counter misses.
                }
                else
                    return false;
            }
        });

        IgniteCache<Integer, Integer> primCache = prim.cache(DEFAULT_CACHE_NAME);

        Consumer<Integer> cachePutAsync = (key) -> GridTestUtils.runAsync(() -> primCache.put(key, key));

        try { // Blocked at primary and backups.
            prepareBlock.set(true);

            blockLatch.set(new CountDownLatch(backupNodes * 20));

            for (int i = 0; i < 20; i++)
                cachePutAsync.accept(++updateCnt);

            blockLatch.get().await();
        }
        finally {
            prepareBlock.set(false);
        }

        try { // Blocked at backups only.
            finishBlock.set(true);

            blockLatch.set(new CountDownLatch(backupNodes * 30));

            for (int i = 0; i < 30; i++)
                cachePutAsync.accept(++updateCnt);

            blockLatch.get().await();
        }
        finally {
            finishBlock.set(false);
        }

        // Making checkpoint, state:
        // - [lwm=2000, missed=[2001 - 2020], hwm=2050] at primary,
        // - [lwm=2000, missed=[], hwm=2000] at backups.
        // Checkpoint will have counter = 2000, but updates [2021 - 2050] will be located before it (after the another "2000 checkpoint").
        // So, [2021 - 2050] will be skipped on rebalancing :(
        forceCheckpoint();

        // Emulating power off, OOM or disk overflow. Keeping data as is, with missed counters updates.
        backups.forEach(
            node -> ((BlockableFileIOFactory)node.configuration().getDataStorageConfiguration().getFileIOFactory()).blocked = true);

        List<String> backNames = backups.stream().map(Ignite::name).collect(Collectors.toList());

        backups.forEach(Ignite::close);

        startGrid(backNames.get(1)); // Restoring any backup.

        // Expected: [2001 - 2050] rebalanced.
        // Actual:
        // - [2001 - 2020] rebalanced.
        // - "Some partition entries were missed during historical rebalance
        //    [grp=CacheGroupContext [grp=default], part=0, missed=30]" at logs
        awaitPartitionMapExchange();

        IdleVerifyResultV2 checkRes = idleVerify(grid(0), DEFAULT_CACHE_NAME);

        // PartitionHashRecordV2 [isPrimary=true, updateCntr=[lwm=2050, missed=[], hwm=2050], partitionState=OWNING,
        // size=2050, partHash=-957578563],
        // PartitionHashRecordV2 [isPrimary=false, updateCntr=[lwm=2050, missed=[], hwm=2050], partitionState=OWNING,
        // size=2020, partHash=868500926]]
        assertFalse(checkRes.hasConflicts());
    }

    /** */
    private static class BlockableFileIOFactory implements FileIOFactory {
        /** IO Factory. */
        private final FileIOFactory factory;

        /** Blocked. */
        public volatile boolean blocked;

        /**
         * @param factory Factory.
         */
        public BlockableFileIOFactory(FileIOFactory factory) {
            this.factory = factory;
        }

        /** {@inheritDoc} */
        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            return new FileIODecorator(factory.create(file, modes)) {
                @Override public int write(ByteBuffer srcBuf) throws IOException {
                    if (blocked)
                        throw new IOException();

                    return super.write(srcBuf);
                }

                @Override public int write(ByteBuffer srcBuf, long position) throws IOException {
                    if (blocked)
                        throw new IOException();

                    return super.write(srcBuf, position);
                }

                @Override public int write(byte[] buf, int off, int len) throws IOException {
                    if (blocked)
                        throw new IOException();

                    return super.write(buf, off, len);
                }
            };
        }
    }
}
