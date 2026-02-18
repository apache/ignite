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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreSession;
import org.apache.ignite.cache.store.CacheStoreSessionListener;
import org.apache.ignite.cache.store.jdbc.CacheJdbcStoreSessionListener;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.AbstractFailureHandler;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.MapCacheStoreStrategy;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;
import org.junit.runners.Parameterized;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/** */
public class IdleVerifyCheckWithWriteThroughTest extends GridCommandHandlerClusterPerMethodAbstractTest {
    /** */
    private AtomicReference<Throwable> err = new AtomicReference<>();

    /** Node kill trigger. */
    private static CountDownLatch nodeKillLatch;

    /** Tx message flag. */
    private static volatile boolean finalTxMsgPassed;

    /** Session method flag. */
    private static AtomicBoolean sessionTriggered = new AtomicBoolean();

    /** Storage exception message. */
    private static final String storageExceptionMessage = "Internal storage exception raised";

    /** */
    @Parameterized.Parameter(1)
    public Boolean withPersistence;

    /** */
    @Parameterized.Parameter(2)
    public static Boolean failOnSessionStart;

    /** */
    @Parameterized.Parameters(name = "cmdHnd={0}, withPersistence={1}, failOnSessionStart={2}")
    public static Collection<Object[]> parameters() {
        return List.of(
            new Object[] {CLI_CMD_HND, false, false},
            new Object[] {CLI_CMD_HND, false, true},
            new Object[] {CLI_CMD_HND, true, false},
            new Object[] {CLI_CMD_HND, true, true}
        );
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        persistenceEnable(withPersistence);

        if (withPersistence)
            cleanPersistenceDir();

        nodeKillLatch = new CountDownLatch(1);
        sessionTriggered = new AtomicBoolean();
        finalTxMsgPassed = false;
    }

    /** {@inheritDoc} */
    @Override protected boolean persistenceEnable() {
        return withPersistence;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
                .setCommunicationSpi(new TestRecordingCommunicationSpi())
                .setFailureHandler(new AbstractFailureHandler() {
                    @Override protected boolean handle(Ignite ignite, FailureContext failureCtx) {
                        err.compareAndSet(null, failureCtx.error());

                        return false;
                    }
                });
    }

    /** Test scenario:
     * <ul>
     *   <li>Start 3 nodes [node0, node1, node2].</li>
     *   <li>Initialize put operation into transactional cache where [node1] holds primary partition for such insertion.</li>
     *   <li>Kill [node1] right after tx PREPARE stage is completed (it triggers tx recovery procedure).</li>
     * </ul>
     *
     * @see IgniteTxManager#salvageTx(IgniteInternalTx)
     */
    @Test
    public void testTxCoordinatorLeftClusterWithEnabledReadWriteThrough() throws Exception {
        // sequential start is important here
        startGrid(0);
        startGrid(1);
        startGrid(2);

        injectTestSystemOut();

        int gridToStop = 1;

        IgniteEx instanceToStop = grid(gridToStop);
        instanceToStop.cluster().state(ClusterState.ACTIVE);

        TestRecordingCommunicationSpi commSpi =
                (TestRecordingCommunicationSpi)instanceToStop.configuration().getCommunicationSpi();
        commSpi.record(GridDhtTxFinishRequest.class);

        commSpi.blockMessages((node, msg) -> {
            boolean ret = msg instanceof GridDhtTxFinishRequest;

            if (ret) {
                nodeKillLatch.countDown();
                finalTxMsgPassed = true;
            }

            return ret;
        });

        MapCacheStoreStrategy strategy = new MapCacheStoreStrategy();
        Factory<? extends CacheStore<Object, Object>> storeFactory = strategy.getStoreFactory();
        CacheConfiguration<Integer, Object> ccfg = new CacheConfiguration<>("cache");
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setCacheMode(CacheMode.REPLICATED);
        ccfg.setReadThrough(true);
        ccfg.setWriteThrough(true);
        ccfg.setCacheStoreFactory(storeFactory);
        ccfg.setCacheStoreSessionListenerFactories(new TestCacheStoreFactory());

        IgniteCache<Integer, Object> cache = instanceToStop.createCache(ccfg);

        awaitPartitionMapExchange();

        IgniteInternalFuture<Object> stopFut = GridTestUtils.runAsync(() -> {
            nodeKillLatch.await();
            stopGrid(gridToStop);
        });

        // primary key for [node1]
        Integer primaryKey = primaryKey(cache);

        //noinspection EmptyCatchBlock
        try (Transaction tx = instanceToStop.transactions().txStart(OPTIMISTIC, READ_COMMITTED)) {
            cache.put(primaryKey, new Object());
            tx.commit();
        }
        catch (Throwable th) {
            // No op
        }

        stopFut.get(getTestTimeout());
        awaitPartitionMapExchange();

        assertEquals(EXIT_CODE_OK, execute("--port", connectorPort(grid(2)), "--cache", "idle_verify"));

        String out = testOut.toString();

        assertContains(log, out, "The check procedure has failed");
        // Update counters are equal but size is different
        if (withPersistence) {
            assertContains(log, out, "updateCntr=[lwm=0, missed=[], hwm=0], partitionState=OWNING, size=0");
            assertContains(log, out, "updateCntr=[lwm=1, missed=[], hwm=1], partitionState=OWNING, size=1");
        }
        else {
            assertContains(log, out, "updateCntr=1, partitionState=OWNING, size=0");
            assertContains(log, out, "updateCntr=1, partitionState=OWNING, size=1");
        }
        testOut.reset();

        assertNotNull(err.get());
        assertThat(err.get().getMessage(), is(containsString("Committing a transaction has produced runtime exception")));

        if (withPersistence) {
            stopAllGrids();
            startGridsMultiThreaded(3);

            awaitPartitionMapExchange(true, true, null);

            assertEquals(EXIT_CODE_OK, execute("--port", connectorPort(grid(2)), "--cache", "idle_verify"));
            out = testOut.toString();
            // partVerHash are different, thus only regex check here
            Pattern primaryPattern = Pattern.compile("Partition instances: " +
                "\\[PartitionHashRecord" +
                ".*?hwm=1\\], partitionState=OWNING, size=1" +
                ".*?hwm=1\\], partitionState=OWNING, size=1" +
                ".*?hwm=1\\], partitionState=OWNING, size=1");

            boolean matches = primaryPattern.matcher(out).find();
            assertTrue(matches);
        }
    }

    /** */
    private static class TestCacheStoreFactory implements Factory<CacheStoreSessionListener> {
        /** {@inheritDoc} */
        @Override public CacheStoreSessionListener create() {
            return new TestCacheJdbcStoreSessionListener();
        }
    }

    /** */
    private static class TestCacheJdbcStoreSessionListener extends CacheJdbcStoreSessionListener {
        /** {@inheritDoc} */
        @Override public void start() throws IgniteException {
            // No op.
        }

        /** {@inheritDoc} */
        @Override public void onSessionStart(CacheStoreSession ses) {
            // Originally connection need to be initialized here.
            if (failOnSessionStart) {
                if (finalTxMsgPassed && sessionTriggered.compareAndSet(false, true))
                    throw new CacheWriterException(storageExceptionMessage);
            }
        }

        /** {@inheritDoc} */
        @Override public void onSessionEnd(CacheStoreSession ses, boolean commit) {
            if (!failOnSessionStart) {
                if (finalTxMsgPassed && sessionTriggered.compareAndSet(false, true))
                    throw new CacheWriterException(storageExceptionMessage);
            }
        }
    }
}
