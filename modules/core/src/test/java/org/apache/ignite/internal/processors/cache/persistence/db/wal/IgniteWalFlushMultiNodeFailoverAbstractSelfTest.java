/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.db.wal;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.file.OpenOption;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.FsyncModeFileWriteAheadLogManager;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DISABLE_WAL_DURING_REBALANCING;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_WAL_MMAP;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;

/**
 * Tests error recovery while node flushing
 */
public abstract class IgniteWalFlushMultiNodeFailoverAbstractSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int ITRS = 2000;

    /** */
    private AtomicBoolean canFail = new AtomicBoolean();

    /**
     * @return Node count.
     */
    protected abstract int gridCount();

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();

        System.setProperty(IGNITE_WAL_MMAP, Boolean.toString(mmap()));
        System.setProperty(IGNITE_DISABLE_WAL_DURING_REBALANCING, "false");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        System.clearProperty(IGNITE_WAL_MMAP);
        System.clearProperty(IGNITE_DISABLE_WAL_DURING_REBALANCING);

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 60_000;
    }

    /**
     * @return WAL mode used in test.
     */
    protected abstract WALMode walMode();

    /**
     * @return {@code True} if test should use MMAP buffer mode.
     */
    protected boolean mmap() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);

        cfg.setCacheConfiguration(
            new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setAtomicityMode(TRANSACTIONAL)
                .setBackups(1)
                .setRebalanceMode(SYNC)
                .setAffinity(new RendezvousAffinityFunction(false, 32))
        );

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setMaxSize(2048L * 1024 * 1024)
                        .setPersistenceEnabled(true))
                .setWalMode(walMode())
                .setWalSegmentSize(512 * 1024)
                .setWalBufferSize(512 * 1024));

        cfg.setFailureHandler(new StopNodeFailureHandler());

        return cfg;
    }

    /**
     * Test flushing error recovery when flush is triggered while node starting
     *
     * @throws Exception In case of fail
     */
    public void testFailWhileStart() throws Exception {
        failWhilePut(true);
    }

    /**
     * Test flushing error recovery when flush is triggered after node started
     *
     * @throws Exception In case of fail
     */
    public void testFailAfterStart() throws Exception {
        failWhilePut(false);
    }

    /**
     * @throws Exception if failed.
     */
    private void failWhilePut(boolean failWhileStart) throws Exception {
        Ignite ig = startGrids(gridCount());

        ig.cluster().active(true);

        IgniteCache<Object, Object> cache = ig.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < ITRS; i++) {
            while (!Thread.currentThread().isInterrupted()) {
                try (Transaction tx = ig.transactions().txStart(
                        TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
                    cache.put(i, "testValue" + i);

                    tx.commit();

                    break;
                }
                catch (Exception expected) {
                    // Expected exception.
                }
            }

            if (i == ITRS / 4) {
                try {
                    if (failWhileStart)
                        canFail.set(true);

                    startGrid(gridCount());

                    setFileIOFactory(grid(gridCount()).context().cache().context().wal());

                    ig.cluster().setBaselineTopology(ig.cluster().topologyVersion());

                    awaitPartitionMapExchange();
                }
                catch (Throwable expected) {
                    // There can be any exception. Do nothing.
                }
            }

            if (i == ITRS / 2)
                canFail.set(true);
        }

        // We should await successful stop of node.
        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return ig.cluster().nodes().size() == gridCount();
            }
        }, getTestTimeout());

        stopAllGrids();

        canFail.set(false);

        Ignite grid0 = startGrids(gridCount() + 1);

        grid0.cluster().active(true);

        cache = grid0.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < ITRS; i++)
            assertEquals(cache.get(i), "testValue" + i);
    }

    /** */
    private void setFileIOFactory(IgniteWriteAheadLogManager wal) {
        if (wal instanceof FileWriteAheadLogManager)
            ((FileWriteAheadLogManager)wal).setFileIOFactory(new FailingFileIOFactory(canFail));
        else if (wal instanceof FsyncModeFileWriteAheadLogManager)
            ((FsyncModeFileWriteAheadLogManager)wal).setFileIOFactory(new FailingFileIOFactory(canFail));
        else
            fail(wal.getClass().toString());
    }

    /**
     * Create File I/O which fails after second attempt to write to File
     */
    private static class FailingFileIOFactory implements FileIOFactory {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final AtomicBoolean fail;

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
                /** {@inheritDoc} */
                @Override public int write(ByteBuffer srcBuf) throws IOException {
                    if (fail != null && file.getName().endsWith(".wal") && fail.get())
                        throw new IOException("No space left on device");

                    return super.write(srcBuf);
                }

                /** {@inheritDoc} */
                @Override public MappedByteBuffer map(int sizeBytes) throws IOException {
                    if (fail != null && file.getName().endsWith(".wal") && fail.get())
                        throw new IOException("No space left on deive");

                    return delegate.map(sizeBytes);
                }
            };
        }
    }
}
