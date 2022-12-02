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

package org.apache.ignite.util;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
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
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;

/** */
public class GridCommandHandlerConsistencyOnClusterCrushTest extends GridCommandHandlerClusterPerMethodAbstractTest {
    /** Listening logger. */
    protected final ListeningTestLogger listeningLog = new ListeningTestLogger(log);

    /** Number of cluster nodes. */
    public int nodes = 3;

    /** Number of backups for the default cache. */
    public int backupNodes = nodes - 1;

    /** */
    @Before public void beforeEachTest() throws Exception {
        cleanPersistenceDir();

        injectTestSystemOut();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setGridLogger(listeningLog)
            .setFailureHandler(new StopNodeFailureHandler());

        cfg.getDataStorageConfiguration()
            .setFileIOFactory(new ThrowableFileIOFactory(cfg.getDataStorageConfiguration().getFileIOFactory()))
            .setWalMode(WALMode.FSYNC); // Allows to use special IO at WAL as well.

        cfg.setCacheConfiguration(new CacheConfiguration<>()
            .setAffinity(new RendezvousAffinityFunction(false, 1))
            .setBackups(backupNodes)
            .setName(DEFAULT_CACHE_NAME)
            .setAtomicityMode(TRANSACTIONAL)
            .setWriteSynchronizationMode(FULL_SYNC) // Allows to be sure that all messages are sent when put succeed.
            .setReadFromBackup(true)); // Allows to check values on backups for idle_verify.

        return cfg;
    }

    /** */
    @Test
    public void testLwmCountersOnClusterCrash() throws Exception {
        int prepareRqNum = 20;
        int finishRqNum = 30;

        IgniteEx ignite = startGrids(nodes);
        ignite.cluster().state(ClusterState.ACTIVE);

        final AtomicInteger updateCnt = new AtomicInteger();

        // Enough to have historical rebalance when needed.
        for (int i = 0; i < 2_000; i++)
            ignite.cache(DEFAULT_CACHE_NAME).put(updateCnt.incrementAndGet(), 0);

        stopAllGrids();
        startGrids(nodes);

        Ignite prim = primaryNode(0L, DEFAULT_CACHE_NAME);

        CountDownLatch prepareLatch = new CountDownLatch(backupNodes * prepareRqNum);
        CountDownLatch finishLatch = new CountDownLatch(backupNodes * finishRqNum);

        TestRecordingCommunicationSpi.spi(prim).blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode node, Message msg) {
                if (msg instanceof GridDhtTxPrepareRequest && prepareLatch.getCount() > 0) {
                    prepareLatch.countDown();

                    return true;
                }
                else if (msg instanceof GridDhtTxFinishRequest && finishLatch.getCount() > 0) {
                    finishLatch.countDown();

                    return true;
                }
                else
                    return false;
            }
        });

        IgniteCache<Integer, Integer> primCache = prim.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < prepareRqNum; i++)
            GridTestUtils.runAsync(() -> primCache.put(updateCnt.incrementAndGet(), 0));

        prepareLatch.await(getTestTimeout(), TimeUnit.MILLISECONDS);

        for (int i = 0; i < finishRqNum; i++)
            GridTestUtils.runAsync(() -> primCache.put(updateCnt.incrementAndGet(), 0));

        finishLatch.await(getTestTimeout(), TimeUnit.MILLISECONDS);

        // Emulating power off, OOM or disk overflow. Keeping data as is, with missed counters updates.
        G.allGrids().forEach(
                node -> ((ThrowableFileIOFactory)node.configuration().getDataStorageConfiguration().getFileIOFactory()).throwIoEx = true);

        stopAllGrids();
        startGrids(nodes);

        awaitPartitionMapExchange();

        assertEquals(EXIT_CODE_OK, execute("--cache", "idle_verify"));
        assertConflicts(false, false);
    }

    /**
     * Checks idle_vefify result.
     *
     * @param counter Counter conflicts.
     * @param hash    Hash conflicts.
     */
    private void assertConflicts(boolean counter, boolean hash) {
        if (counter || hash)
            assertContains(log, testOut.toString(), "conflict partitions has been found: " +
                    "[counterConflicts=" + (counter ? 1 : 0) + ", hashConflicts=" + (hash ? 1 : 0) + "]");
        else
            assertContains(log, testOut.toString(), "no conflicts have been found");
    }

    /** */
    private static class ThrowableFileIOFactory implements FileIOFactory {
        /** IO Factory. */
        private final FileIOFactory factory;

        /** Throw exception to emulate an error. */
        public volatile boolean throwIoEx;

        /**
         * @param factory Factory.
         */
        public ThrowableFileIOFactory(FileIOFactory factory) {
            this.factory = factory;
        }

        /** {@inheritDoc} */
        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            return new FileIODecorator(factory.create(file, modes)) {
                @Override public int write(ByteBuffer srcBuf) throws IOException {
                    if (throwIoEx)
                        throw new IOException("Test exception");

                    return super.write(srcBuf);
                }

                @Override public int write(ByteBuffer srcBuf, long position) throws IOException {
                    if (throwIoEx)
                        throw new IOException("Test exception");

                    return super.write(srcBuf, position);
                }

                @Override public int write(byte[] buf, int off, int len) throws IOException {
                    if (throwIoEx)
                        throw new IOException("Test exception");

                    return super.write(buf, off, len);
                }
            };
        }
    }
}
