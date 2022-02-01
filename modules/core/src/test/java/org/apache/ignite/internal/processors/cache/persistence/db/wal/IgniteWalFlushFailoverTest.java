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
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.GridKernalState;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

/**
 *
 */
public class IgniteWalFlushFailoverTest extends GridCommonAbstractTest {
    /** */
    private static final String TEST_CACHE = "testCache";

    /** */
    private boolean flushByTimeout;

    /** */
    private AtomicBoolean canFail = new AtomicBoolean();

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 30_000L;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration cacheCfg = new CacheConfiguration(TEST_CACHE)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        cfg.setCacheConfiguration(cacheCfg);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                 new DataRegionConfiguration().setMaxSize(2048L * 1024 * 1024).setPersistenceEnabled(true))
                .setWalMode(WALMode.BACKGROUND)
                .setWalBufferSize(1024 * 1024)// Setting WAL Segment size to high values forces flushing by timeout.
                .setWalSegmentSize(flushByTimeout ? 2 * 1024 * 1024 : 512 * 1024);

        cfg.setDataStorageConfiguration(memCfg);

        return cfg;
    }

    /**
     * Test flushing error recovery when flush is triggered asynchronously by timeout
     *
     * @throws Exception In case of fail
     */
    @Test
    public void testErrorOnFlushByTimeout() throws Exception {
        flushByTimeout = true;
        flushingErrorTest();
    }

    /**
     * Test flushing error recovery when flush is triggered directly by transaction commit
     *
     * @throws Exception In case of fail
     */
    @Test
    public void testErrorOnDirectFlush() throws Exception {
        flushByTimeout = false;
        flushingErrorTest();
    }

    /**
     * @throws Exception if failed.
     */
    private void flushingErrorTest() throws Exception {
        final IgniteEx grid = startGrid(0);

        FileWriteAheadLogManager wal = (FileWriteAheadLogManager)grid.context().cache().context().wal();

        boolean mmap = GridTestUtils.getFieldValue(wal, "mmap");

        if (mmap)
            return;

        wal.setFileIOFactory(new FailingFileIOFactory(canFail));

        try {
            grid.active(true);

            IgniteCache<Object, Object> cache = grid.cache(TEST_CACHE);

            final int iterations = 100;

            canFail.set(true);

            for (int i = 0; i < iterations; i++) {
                Transaction tx = grid.transactions().txStart(
                    TransactionConcurrency.PESSIMISTIC, TransactionIsolation.READ_COMMITTED);

                cache.put(i, "testValue" + i);

                Thread.sleep(100L);

                tx.commitAsync().get();
            }
        }
        catch (Exception expected) {
            // There can be any exception. Do nothing.
        }

        // We should await successful stop of node.
        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return grid.context().gateway().getState() == GridKernalState.STOPPED;
            }
        }, getTestTimeout());
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
                int writeAttempts = 2;

                @Override public int write(ByteBuffer srcBuf) throws IOException {

                    if (--writeAttempts <= 0 && fail != null && fail.get())
                        throw new IOException("No space left on device");

                    return super.write(srcBuf);
                }

                /** {@inheritDoc} */
                @Override public MappedByteBuffer map(int sizeBytes) throws IOException {
                    return delegate.map(sizeBytes);
                }
            };
        }
    }
}
