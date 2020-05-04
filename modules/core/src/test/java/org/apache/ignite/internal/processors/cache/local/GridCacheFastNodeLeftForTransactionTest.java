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

package org.apache.ignite.internal.processors.cache.local;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.StreamHandler;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishFuture;
import org.apache.ignite.internal.processors.cache.transactions.TransactionProxyImpl;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.Objects.nonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.IntStream.range;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;
import static org.apache.ignite.internal.commandline.CommandHandler.initLogger;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;
import static org.apache.ignite.testframework.GridTestUtils.setFieldValue;
import static org.apache.ignite.testframework.LogListener.matches;

/**
 * Class for testing fast node left during transaction for cache.
 */
public class GridCacheFastNodeLeftForTransactionTest extends GridCommonAbstractTest {
    /** Number of nodes. */
    private static final int NODES = 4;

    /** Number of transactions. */
    private static final int TX_COUNT = 20;

    /** Logger for listen log messages. */
    private static ListeningTestLogger listeningLog;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        listeningLog = new ListeningTestLogger(false, GridAbstractTest.log);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        /*To listen the logs of future in current tests, since the log in the
        futures is static and is not reset when tests are launched.*/
        setFieldValue(GridDhtTxFinishFuture.class, "log", null);
        ((AtomicReference<IgniteLogger>)getFieldValue(GridDhtTxFinishFuture.class, "logRef")).set(null);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        listeningLog.clearListeners();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(createCacheConfigs())
            .setGridLogger(listeningLog)
            .setConnectorConfiguration(new ConnectorConfiguration());
    }

    /**
     * Test transaction rollback when one of the nodes drops out.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRollbackTransactions() throws Exception {
        int txCnt = TX_COUNT;

        int nodes = NODES;

        IgniteEx crd = createCluster(nodes);

        for (CacheConfiguration cacheConfig : createCacheConfigs()) {
            String cacheName = cacheConfig.getName();

            IgniteCache<Object, Object> cache = crd.cache(cacheName);

            List<Integer> keys = primaryKeys(cache, txCnt);

            Map<Integer, Integer> cacheValues = range(0, txCnt / 2).boxed().collect(toMap(keys::get, identity()));

            cache.putAll(cacheValues);

            Collection<Transaction> txs = createTxs(
                grid(nodes),
                cacheName,
                range(txCnt / 2, txCnt).mapToObj(keys::get).collect(toList())
            );

            int stoppedNodeId = 2;

            stopGrid(stoppedNodeId);

            LogListener logLsnr = newLogListener();

            listeningLog.registerListener(logLsnr);

            for (Transaction tx : txs)
                tx.rollback();

            awaitPartitionMapExchange();

            check(cacheValues, cacheName, logLsnr, stoppedNodeId);
        }
    }

    /**
     * Test for rollback transactions when one of the nodes drops out,
     * with operations performed on keys outside the transaction.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRollbackTransactionsWithKeyOperationOutsideThem() throws Exception {
        int txCnt = TX_COUNT;

        int nodes = NODES;

        IgniteEx crd = createCluster(nodes);

        for (CacheConfiguration cacheConfig : createCacheConfigs()) {
            String cacheName = cacheConfig.getName();

            IgniteCache<Object, Object> cache = crd.cache(cacheName);

            List<Integer> keys = primaryKeys(cache, txCnt);

            Map<Integer, Integer> cacheValues = range(0, txCnt / 2).boxed().collect(toMap(keys::get, identity()));

            cache.putAll(cacheValues);

            List<Integer> txKeys = range(txCnt / 2, txCnt).mapToObj(keys::get).collect(toList());

            IgniteEx clientNode = grid(nodes);

            Collection<Transaction> txs = createTxs(clientNode, cacheName, txKeys);

            int stoppedNodeId = 2;

            stopGrid(stoppedNodeId);

            CountDownLatch latch = new CountDownLatch(1);

            GridTestUtils.runAsync(() -> {
                latch.countDown();

                IgniteCache<Object, Object> clientCache = clientNode.cache(DEFAULT_CACHE_NAME);

                txKeys.forEach(clientCache::get);
            });

            LogListener logLsnr = newLogListener();

            listeningLog.registerListener(logLsnr);

            latch.await();

            for (Transaction tx : txs)
                tx.rollback();

            awaitPartitionMapExchange();

            check(cacheValues, cacheName, logLsnr, stoppedNodeId);
        }
    }

    /**
     * Checking the contents of the cache after rollback transactions,
     * with restarting the stopped node with using "idle_verify".
     *
     * @param cacheValues Expected cache contents.
     * @param cacheName Cache name.
     * @param logLsnr LogListener.
     * @param stoppedNodeId ID of the stopped node.
     * @throws Exception If failed.
     */
    private void check(
        Map<Integer, Integer> cacheValues,
        String cacheName,
        LogListener logLsnr,
        int stoppedNodeId
    ) throws Exception {
        assert nonNull(cacheValues);
        assert nonNull(cacheName);
        assert nonNull(logLsnr);

        checkCacheData(cacheValues, cacheName);

        assertTrue(logLsnr.check());

        startGrid(stoppedNodeId);

        awaitPartitionMapExchange();

        checkCacheData(cacheValues, cacheName);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        PrintStream sysOut = System.out;

        try (PrintStream out = new PrintStream(baos)) {
            System.setOut(out);

            Logger cmdLog = createTestLogger(baos);
            CommandHandler cmdHnd = new CommandHandler(cmdLog);

            cmdHnd.execute(asList("--cache", "idle_verify"));

            stream(cmdLog.getHandlers()).forEach(Handler::flush);

            assertContains(listeningLog, baos.toString(), "no conflicts have been found");
        }
        finally {
            System.setOut(sysOut);
        }
    }

    /**
     * Creating a logger for a CommandHandler.
     *
     * @param outputStream Stream for recording the result of a command.
     * @return Logger.
     */
    private Logger createTestLogger(OutputStream outputStream) {
        assert nonNull(outputStream);

        Logger log = initLogger(null);

        log.addHandler(new StreamHandler(outputStream, new Formatter() {
            @Override public String format(LogRecord record) {
                return record.getMessage() + "\n";
            }
        }));

        return log;
    }

    /**
     * Creating a cluster.
     *
     * @param nodes Number of server nodes, plus one client.
     * @throws Exception If failed.
     */
    private IgniteEx createCluster(int nodes) throws Exception {
        IgniteEx crd = startGrids(nodes);

        startClientGrid(nodes);

        awaitPartitionMapExchange();

        return crd;
    }

    /**
     * Transaction creation.
     *
     * @param node Node.
     * @param cacheName Cache name.
     * @param keys Keys.
     * @return Transactions.
     * @throws Exception If failed.
     */
    private Collection<Transaction> createTxs(
        IgniteEx node,
        String cacheName,
        Collection<Integer> keys
    ) throws Exception {
        assert nonNull(node);
        assert nonNull(cacheName);
        assert nonNull(keys);

        IgniteCache<Object, Object> cache = node.cache(cacheName);

        Collection<Transaction> txs = new ArrayList<>();

        for (Integer key : keys) {
            Transaction tx = node.transactions().txStart();

            cache.put(key, key + 10);

            ((TransactionProxyImpl)tx).tx().prepare(true);

            txs.add(tx);
        }

        return txs;
    }

    /**
     * Creating an instance of LogListener to find an exception
     * "Unable to send message (node left topology):".
     *
     * @return LogListener.
     */
    private LogListener newLogListener() {
        return matches("Unable to send message (node left topology):").build();
    }

    /**
     * Creating a cache configurations.
     *
     * @return Cache configurations.
     */
    private CacheConfiguration[] createCacheConfigs() {
        return new CacheConfiguration[] {
            createCacheConfig(DEFAULT_CACHE_NAME + "_0", FULL_SYNC),
            createCacheConfig(DEFAULT_CACHE_NAME + "_1", PRIMARY_SYNC)
        };
    }

    /**
     * Creating a cache configuration.
     *
     * @param cacheName Cache name.
     * @param syncMode Sync mode.
     * @return Cache configuration.
     */
    private CacheConfiguration createCacheConfig(String cacheName, CacheWriteSynchronizationMode syncMode) {
        assert nonNull(cacheName);
        assert nonNull(syncMode);

        return new CacheConfiguration(cacheName)
            .setAtomicityMode(TRANSACTIONAL)
            .setBackups(2)
            .setAffinity(new RendezvousAffinityFunction(false, 10))
            .setWriteSynchronizationMode(syncMode);
    }
}
