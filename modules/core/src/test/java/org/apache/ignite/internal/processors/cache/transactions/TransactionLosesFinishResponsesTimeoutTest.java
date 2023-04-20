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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/**
 * This test shows how a transaction may not timeout on commit (after prepared) phase.
 * Instead of expected transaction timeout we meet the test timeout while the transaction is infinite.
 * User should know at least what the timeout is, how and when it works (not includes commit).
 * Or even better we should finish the awaiting future and transaction with some warnings 'Like long commit detected.
 */
public class TransactionLosesFinishResponsesTimeoutTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setTransactionConfiguration(new TransactionConfiguration()
            .setDefaultTxConcurrency(PESSIMISTIC)
            .setDefaultTxIsolation(READ_COMMITTED)
            .setDefaultTxTimeout(5_000)
        );

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrids(3);
    }

    /** */
    @Test
    public void testBackupFinishResponseLost() throws Exception {
        test(() -> blockMessage(backupNodes(0, "cache").get(0), GridDhtTxFinishResponse.class),
            ccfg -> ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC));
    }

    /** This test works. The transaction timeout is processed. */
    //@Test
    public void testBackupPrepareResponseLost() throws Exception {
        test(() -> blockMessage(backupNodes(0, "cache").get(0), GridDhtTxPrepareResponse.class),
            ccfg -> ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC));
    }

    /** */
    @Test
    public void testPrimaryPrepareNearResponseLost() throws Exception {
        test(() -> blockMessage(primaryNode(0, "cache"), GridNearTxPrepareResponse.class),
            ccfg -> ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC));
    }

    /** This test works. The transaction timeout is processed. */
    //@Test
    public void testPrimaryLockResponseLost() throws Exception {
        test(() -> blockMessage(primaryNode(0, "cache"), GridNearLockResponse.class),
            ccfg -> ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC));
    }

    /** */
    @Test
    public void testPrimaryNearFinishResponseLost() throws Exception {
        test(() -> blockMessage(primaryNode(0, "cache"), GridNearTxFinishResponse.class),
            ccfg -> ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC));
    }

    /** */
    private void test(Runnable blockAction, Consumer<CacheConfiguration> fixCcfg) throws Exception {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>("cache")
            .setBackups(2)
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        fixCcfg.accept(ccfg);

        IgniteCache<Object, Object> cache = grid(1).createCache(ccfg);

        preloadCache(cache);

        IgniteEx client = startClientGrid();

        blockAction.run();

        cache = client.cache("cache");

        Map<Object, Object> data = new TreeMap<>();

        for (int i = 0; i < 1; ++i)
            data.put(i, i + 1);

        U.TEST = true;

        try (Transaction tx0 = client.transactions().txStart()) {
            cache.putAll(data);

            tx0.commit();
        } catch (Exception e){
            log.warning("Unable to commit transaction.", e);
        }

        for (int i = 0; i < 10; ++i)
            assertEquals(i, grid(0).cache("cache").get(i));
    }

    /** */
    private void preloadCache(IgniteCache<Object, Object> cache) {
        try (IgniteDataStreamer s = grid(1).dataStreamer("cache")) {
            for (int i = 0; i < 1_000; ++i)
                s.addData(i, i);
        }
    }

    /** */
    private void blockMessage(Ignite grid, Class<? extends Message> cl) {
        ((TestRecordingCommunicationSpi)grid.configuration().getCommunicationSpi())
            .blockMessages((n, m) -> cl.isAssignableFrom(m.getClass()));
    }
}