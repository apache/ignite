/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import java.nio.MappedByteBuffer;
import java.nio.file.OpenOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.filehandle.FileWriteHandle;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.IGNITE_PDS_SKIP_CHECKPOINT_ON_NODE_STOP;
import static org.apache.ignite.testframework.GridTestUtils.DFLT_BUSYWAIT_SLEEP_INTERVAL;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/**
 * A set of tests that check correctness of logical recovery performed during node start.
 */
@RunWith(Parameterized.class)
public class IgniteLogicalRecoveryWithParamsTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setFailureHandler(new StopNodeFailureHandler());

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setWalMode(WALMode.LOG_ONLY)
            .setCheckpointFrequency(1024 * 1024 * 1024) // Disable automatic checkpoints.
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setName("dflt")
                    .setInitialSize(256 * 1024 * 1024)
                    .setMaxSize(256 * 1024 * 1024)
                    .setPersistenceEnabled(true)
            );

        cfg.setDataStorageConfiguration(dsCfg);

        TestRecordingCommunicationSpi spi = new TestRecordingCommunicationSpi();

        cfg.setCommunicationSpi(spi);

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

    /** Parametrized run param : server nodes. */
    @Parameterized.Parameter(0)
    public Integer numSrvNodes;

    /** Parametrized run param : single node tx. */
    @Parameterized.Parameter(1)
    public Boolean singleNodeTx;

    /** Parametrized run param : backups count. */
    @Parameterized.Parameter(2)
    public Integer backups;

    /** Test run configurations: Cache mode, atomicity type, is near. */
    @Parameterized.Parameters(name = "nodesCnt={0}, singleNodeTx={1}, backups={2}")
    public static Collection<Object[]> runConfig() {
        return Arrays.asList(new Object[][] {
            {1, true, 0},
            {1, true, 1},
            {1, false, 0},
            {1, false, 1},
            {2, true, 0},
            {2, true, 1},
            //{2, false, 0}, such case is not fixed by now
            {2, false, 1},
        });
    }

    /**Tests partially commited transactions with further recovery. */
    @Test
    @WithSystemProperty(key = IGNITE_PDS_SKIP_CHECKPOINT_ON_NODE_STOP, value = "true")
    public void testPartiallyCommitedTx_WithCpOnNodeStop() throws Exception {
        testPartiallyCommitedTx();
    }

    /**Tests partially commited transactions with further recovery. */
    @Test
    public void testPartiallyCommitedTx_WithoutCpOnNodeStop() throws Exception {
        testPartiallyCommitedTx();
    }

    /**
     * Tests concurrent tx with node stop and further recovery.
     *
     */
    private void testPartiallyCommitedTx() throws Exception {
        final String cacheName = "recovery";

        int itmsCount = 30_000;

        AtomicBoolean failFileIO = new AtomicBoolean();

        List<Integer> keys;

        CacheConfiguration<Integer, Integer> cfg = new CacheConfiguration<Integer, Integer>(cacheName)
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setBackups(backups)
            .setAffinity(new RendezvousAffinityFunction(false, 32));

        try {
            final IgniteEx srv = (IgniteEx)startGridsMultiThreaded(numSrvNodes);

            G.allGrids().forEach(n -> setWalIOFactory(n, failFileIO));

            IgniteEx clnt = startClientGrid("client");

            TestRecordingCommunicationSpi nearComm = TestRecordingCommunicationSpi.spi(clnt);

            srv.cluster().state(ClusterState.ACTIVE);

            final IgniteCache cache = clnt.getOrCreateCache(cfg);

            final CountDownLatch commitStart = new CountDownLatch(1);

            forceCheckpoint();

            nearComm.blockMessages((node, msg) -> msg instanceof GridNearTxPrepareRequest);

            if (singleNodeTx)
                keys = primaryKeys(srv.cache(cacheName), itmsCount, 0);
            else
                keys = IntStream.range(0, itmsCount).boxed().collect(Collectors.toList());

            Thread t = new Thread(() -> {
                try (Transaction tx = clnt.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
                    keys.forEach(k -> cache.put(k, k));

                    commitStart.countDown();

                    tx.commit();
                }
            });

            t.start();

            commitStart.await();

            nearComm.waitForBlocked();

            nearComm.stopBlock();

            assertTrue(waitForWalUpdates(G.allGrids().stream().filter(g -> !g.configuration().isClientMode())
                .collect(Collectors.toList())));
        }
        finally {
            failFileIO.set(true);

            stopAllGrids(true);

            assertTrue(G.allGrids().isEmpty());
        }

        final IgniteEx srv = (IgniteEx)startGridsMultiThreaded(numSrvNodes);

        srv.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, Integer> cache = srv.cache(cacheName);

        int cSize = cache.size();

        boolean pr = cache.get(keys.get(0)) == null;

        for (int i : keys) {
            Object res = cache.get(i);

            if (pr != (res == null))
                assertEquals("ethalon=" + pr + ", current=" + res + ", key=" + i, pr, res == null);
        }

        assert (cSize == itmsCount || cSize == 0) : "unexpected cache size: " + cSize;
    }

    /** */
    private boolean waitForWalUpdates(Collection<Ignite> grids) throws IgniteInterruptedCheckedException {
        long start = U.currentTimeMillis();

        int[] offsets = new int[grids.size()];

        int gCnt = 0;

        for (Ignite grid : grids)
            offsets[gCnt++] = getWalPos(grid);

        while (true) {
            gCnt = 0;

            for (Ignite grid : grids) {
                if (getWalPos(grid) - offsets[gCnt++] > 100)
                    return true;
            }

            U.sleep(DFLT_BUSYWAIT_SLEEP_INTERVAL / 2);

            if (U.currentTimeMillis() - start > 20_000)
                return false;
        }
    }

    /**
     * Sets file IO factory.
     *
     * @param grid Ignite instance.
     * @param canFail If {@code true} throws exception on write.
     *
     */
    private void setWalIOFactory(Ignite grid, AtomicBoolean canFail) {
        IgniteEx grid0 = (IgniteEx)grid;

        FileWriteAheadLogManager walMgr = (FileWriteAheadLogManager)grid0.context().cache().context().wal();

        walMgr.setFileIOFactory(new FailingFileIOFactory(canFail));
    }

    /**
     * @param grid Ignite instance.
     * @return Returns current wal position.
     */
    private int getWalPos(Ignite grid) {
        IgniteEx grid0 = (IgniteEx)grid;

        FileWriteAheadLogManager walMgr = (FileWriteAheadLogManager)grid0.context().cache().context().wal();

        FileWriteHandle fhAfter = U.field(walMgr, "currHnd");

        try {
            fhAfter.fsync(null);
        }
        catch (IgniteCheckedException e) {
            U.warn(log, e);
        }

        return fhAfter.position().fileOffset();
    }

    /**
     * Create File I/O which fails after flag is touched.
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
                @Override public int write(ByteBuffer srcBuf) throws IOException {
                    if (fail != null && fail.get())
                        throw new IOException("No space left on device");

                    return super.write(srcBuf);
                }

                /** {@inheritDoc} */
                @Override public int write(ByteBuffer srcBuf, long position) throws IOException {
                    if (fail != null && fail.get())
                        throw new IOException("No space left on device");

                    return delegate.write(srcBuf, position);
                }

                /** {@inheritDoc} */
                @Override public int write(byte[] buf, int off, int len) throws IOException {
                    if (fail != null && fail.get())
                        throw new IOException("No space left on device");

                    return delegate.write(buf, off, len);
                }

                /** {@inheritDoc} */
                @Override public MappedByteBuffer map(int sizeBytes) throws IOException {
                    return delegate.map(sizeBytes);
                }
            };
        }
    }
}
