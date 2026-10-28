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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.concurrent.locks.Lock;
import java.util.regex.Pattern;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Tests that the diagnostics of a long held explicit lock show how long the lock has been held and what the thread
 * holding it is doing: in the long running operations dump and in the dump of an exchange blocked by the lock.
 */
public class ExplicitLockDumpTest extends GridCommonAbstractTest {
    /** */
    private static final String OWNER_DUMP = "(?s)>>> GridCacheExplicitLockSpan \\[.*holdTime=[1-9].*" +
        "Stack trace of the explicit lock owner thread:.*ExplicitLockDumpTest\\.";

    /** */
    private final ListeningTestLogger listeningLog = new ListeningTestLogger(log);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setGridLogger(listeningLog)
            // 2 * networkTimeout is the pending objects dump timeout of the exchange, see
            // GridDhtPartitionsExchangeFuture#waitPartitionRelease and ExchangeWorker#body.
            .setNetworkTimeout(1000)
            .setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME).setAtomicityMode(TRANSACTIONAL));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** The lock held longer than the long operations dump timeout is dumped without an exchange. */
    @Test
    @WithSystemProperty(key = IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT, value = "500")
    public void testLongRunningDump() throws Exception {
        Lock lock = startGrid(0).cache(DEFAULT_CACHE_NAME).lock(0);

        LogListener lsnr = LogListener.matches(Pattern.compile("First \\d+ long running explicit locks \\[total=1\\]"))
            .andMatches(Pattern.compile(OWNER_DUMP + "testLongRunningDump\\(")).build();

        listeningLog.registerListener(lsnr);

        lock.lock();

        try {
            assertTrue(waitForCondition(lsnr::check, getTestTimeout()));
        }
        finally {
            lock.unlock();
        }
    }

    /** The server waits for its own lock on partition release. */
    @Test
    public void testServerLockOwnerDump() throws Exception {
        checkOwnerDump(startGrid(0));
    }

    /** The client does not wait for partition release, its exchange worker dumps the lock when the exchange hangs. */
    @Test
    public void testClientLockOwnerDump() throws Exception {
        startGrid(0);

        checkOwnerDump(startClientGrid(1));
    }

    /** The lock is left by a terminated thread, so there is no stack trace to show. */
    @Test
    public void testLeakedLockDump() throws Exception {
        IgniteEx ignite = startGrid(0);

        runAsync(() -> ignite.cache(DEFAULT_CACHE_NAME).lock(0).lock()).get();

        checkExchangeDump("Thread \\[id=\\d+\\] is not alive");
    }

    /** @param lockNode Node that acquires the lock in the test thread. */
    private void checkOwnerDump(IgniteEx lockNode) throws Exception {
        Lock lock = lockNode.cache(DEFAULT_CACHE_NAME).lock(0);

        lock.lock();

        IgniteInternalFuture<?> fut;

        try {
            fut = checkExchangeDump(OWNER_DUMP + "checkOwnerDump\\(");
        }
        finally {
            lock.unlock();
        }

        fut.get(getTestTimeout());
    }

    /** Starts a cache, which is blocked by the lock, and waits for the exchange diagnostics. */
    private IgniteInternalFuture<?> checkExchangeDump(String regex) throws Exception {
        LogListener lsnr = LogListener.matches(Pattern.compile(regex)).build();

        listeningLog.registerListener(lsnr);

        IgniteInternalFuture<?> fut = runAsync(() -> grid(0).createCache("blocked"));

        assertTrue(regex, waitForCondition(lsnr::check, getTestTimeout()));
        assertFalse(fut.isDone());

        return fut;
    }
}
