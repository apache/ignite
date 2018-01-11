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
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import java.nio.file.OpenOption;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/**
 * Tests error recovery while node flushing
 */
public abstract class IgniteWalFlushMultiNodeFailoverAbstractSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String TEST_CACHE = "testCache";

    /** */
    private static final int ITRS = 1000;

    /** */
    private AtomicBoolean canFail = new AtomicBoolean();

    /**
     * @return Node count.
     */
    protected abstract int gridCount();

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        deleteWorkFiles();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        deleteWorkFiles();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 30_000;
    }

    /** {@inheritDoc} */
    protected abstract WALMode walMode();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration cacheCfg = new CacheConfiguration(TEST_CACHE)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setBackups(1);

        cfg.setCacheConfiguration(cacheCfg);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration().setMaxSize(2048L * 1024 * 1024).setPersistenceEnabled(true))
                .setWalMode(this.walMode())
                .setWalSegmentSize(50_000);

        if (gridName.endsWith(String.valueOf(gridCount())))
            memCfg.setFileIOFactory(new FailingFileIOFactory(canFail));

        cfg.setDataStorageConfiguration(memCfg);

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
    public void failWhilePut(boolean failWhileStart) throws Exception {

        final Ignite grid = startGridsMultiThreaded(gridCount());

        IgniteWriteAheadLogManager wal = ((IgniteKernal)grid).context().cache().context().wal();

        boolean mmap = GridTestUtils.getFieldValue(wal, "mmap");

        if (mmap)
            return;

        grid.active(true);

        IgniteCache<Object, Object> cache = grid.cache(TEST_CACHE);

        for (int i = 0; i < ITRS; i++) {
            while (true) {
                try (Transaction tx = grid.transactions().txStart(
                        TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED)) {
                    cache.put(i, "testValue" + i);

                    tx.commit();

                    break;
                } catch (Exception expected) {
                    // Expected exception.
                }
            }

            if (i == ITRS / 4) {
                try {
                    if (failWhileStart)
                        canFail.set(true);

                    startGrid(gridCount());

                    waitForRebalancing();
                } catch (Exception expected) {
                    // There can be any exception. Do nothing.
                }
            }

            if (i == ITRS / 2)
                canFail.set(true);
        }


        // We should await successful stop of node.
        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return grid.cluster().nodes().size() == gridCount();
            }
        }, getTestTimeout());

        stopAllGrids();

        Ignite grid0 = startGrids(gridCount() + 1);

        grid0.active(true);

        cache = grid0.cache(TEST_CACHE);

        for (int i = 0; i < ITRS; i++)
            assertEquals(cache.get(i), "testValue" + i);
    }


    /**
     * @throws IgniteCheckedException
     */
    private void deleteWorkFiles() throws IgniteCheckedException {
        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false));
    }

    /**
     * Create File I/O which fails after second attempt to write to File
     */
    private static class FailingFileIOFactory implements FileIOFactory {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private AtomicBoolean fail;

        /** */
        private final FileIOFactory delegateFactory = new RandomAccessFileIOFactory();

        /** */
        FailingFileIOFactory(AtomicBoolean fail) {
            this.fail = fail;
        }

        /** {@inheritDoc} */
        @Override public FileIO create(File file) throws IOException {
            return create(file, CREATE, READ, WRITE);
        }

        /** {@inheritDoc} */
        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            final FileIO delegate = delegateFactory.create(file, modes);

            return new FileIODecorator(delegate) {
                int writeAttempts = 2;

                @Override public int write(ByteBuffer srcBuf) throws IOException {
                    if (--writeAttempts == 0 && fail!= null && fail.get())
                        throw new IOException("No space left on device");

                    return super.write(srcBuf);
                }

                /** {@inheritDoc} */
                @Override public MappedByteBuffer map(int maxWalSegmentSize) throws IOException {
                    return delegate.map(maxWalSegmentSize);
                }
            };
        }
    }
}
