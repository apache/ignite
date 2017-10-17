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
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.GridKernalState;
import org.apache.ignite.internal.IgniteEx;
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

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/**
 *
 */
public class IgniteWalFlushFailoverTest extends GridCommonAbstractTest {
    /** */
    private static final String TEST_CACHE = "testCache";

    /** */
    private boolean flushByTimeout;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        deleteWorkFiles();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        deleteWorkFiles();
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

        MemoryPolicyConfiguration memPlcCfg = new MemoryPolicyConfiguration()
                .setName("dfltMemPlc")
                .setInitialSize(2 * 1024L * 1024L * 1024L);

        MemoryConfiguration memCfg = new MemoryConfiguration()
                .setMemoryPolicies(memPlcCfg)
                .setDefaultMemoryPolicyName(memPlcCfg.getName());

        cfg.setMemoryConfiguration(memCfg);

        PersistentStoreConfiguration storeCfg = new PersistentStoreConfiguration()
                .setFileIOFactory(new FailingFileIOFactory())
                .setWalMode(WALMode.BACKGROUND)
                // Setting WAL Segment size to high values forces flushing by timeout.
                .setWalSegmentSize(flushByTimeout ? 500_000 : 50_000);

        cfg.setPersistentStoreConfiguration(storeCfg);

        return cfg;
    }

    /**
     * Test flushing error recovery when flush is triggered asynchronously by timeout
     *
     * @throws Exception In case of fail
     */
    public void testErrorOnFlushByTimeout() throws Exception {
        flushByTimeout = true;
        flushingErrorTest();
    }

    /**
     * Test flushing error recovery when flush is triggered directly by transaction commit
     *
     * @throws Exception In case of fail
     */
    public void testErrorOnDirectFlush() throws Exception {
        flushByTimeout = false;
        flushingErrorTest();
    }

    /**
     * @throws Exception if failed.
     */
    private void flushingErrorTest() throws Exception {
        final IgniteEx grid = startGrid(0);
        grid.active(true);

        IgniteCache<Object, Object> cache = grid.cache(TEST_CACHE);

        final int iterations = 100;

        try {
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
            @Override
            public boolean apply() {
                return grid.context().gateway().getState() == GridKernalState.STOPPED;
            }
        }, getTestTimeout());
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
        private static final long serialVersionUID = 0L;

        /** */
        private final FileIOFactory delegateFactory = new RandomAccessFileIOFactory();

        /** {@inheritDoc} */
        @Override public FileIO create(File file) throws IOException {
            return create(file, CREATE, READ, WRITE);
        }

        /** {@inheritDoc} */
        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            FileIO delegate = delegateFactory.create(file, modes);

            return new FileIODecorator(delegate) {
                int writeAttempts = 2;

                @Override public int write(ByteBuffer sourceBuffer) throws IOException {
                    if (--writeAttempts == 0)
                        throw new RuntimeException("Test exception. Unable to write to file.");

                    return super.write(sourceBuffer);
                }
            };
        }
    }
}
