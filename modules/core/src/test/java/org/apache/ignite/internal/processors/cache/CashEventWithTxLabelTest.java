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

package org.apache.ignite.internal.processors.cache;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.util.typedef.PAX;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Assert;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_READ;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_REMOVED;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Test pass transaction's label for EVT_CACHE_OBJECT_READ, EVT_CACHE_OBJECT_PUT, EVT_CACHE_OBJECT_REMOVED events
 */
public class CashEventWithTxLabelTest extends GridCommonAbstractTest {

    /** types event to be checked */
    private static final int[] CACHE_EVENT_TYPES = {EVT_CACHE_OBJECT_READ, EVT_CACHE_OBJECT_PUT, EVT_CACHE_OBJECT_REMOVED};

    /** Transaction label */
    private static final String TX_LABEL = "TX_LABEL";

    /** number of server nodes */
    private static final int SRVS = 2;

    /** number of client nodes */
    private static final int CLIENTS = 1;

    /** cache name */
    public static final String CACHE_NAME = "cache";

    /** */
    private boolean client;

    private Integer primaryKey = 0;
    private Integer backupKey = 0;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClientMode(client);

        TcpCommunicationSpi commSpi = new TcpCommunicationSpi() {
            @Override public void sendMessage(ClusterNode node, Message msg) throws IgniteSpiException {
                traceMessage(node, msg);

                super.sendMessage(node, msg);
            }

            @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
                throws IgniteSpiException {
                traceMessage(node, msg);

                super.sendMessage(node, msg, ackC);
            }

            private void traceMessage(ClusterNode node, Message msg) {
                if (!(msg instanceof GridIoMessage))
                    return;

                Message msg0 = ((GridIoMessage)msg).message();

                GridKernalContext ctx = ((IgniteKernal)this.ignite()).context();

                String locNodeName = ctx.igniteInstanceName();
                String rmtNodeName = node.attribute(ATTR_IGNITE_INSTANCE_NAME);

                System.out.println(">>> SEND " + locNodeName + " -> " + rmtNodeName + ": " + msg0 + ']');
            }
        };

        cfg.setCommunicationSpi(commSpi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTestsStarted();

        client = false;

        startGridsMultiThreaded(SRVS);

        client = true;

        startGridsMultiThreaded(SRVS, CLIENTS);

        client = false;

        client().createCache(
            new CacheConfiguration<Integer, Integer>()
                .setName("cache")
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setCacheMode(CacheMode.PARTITIONED)
                .setBackups(backups())
        );

        while (!client().affinity("cache").isPrimary(((IgniteKernal)primary()).localNode(), primaryKey))
            primaryKey++;

        while (!client().affinity("cache").isBackup(((IgniteKernal)primary()).localNode(), backupKey))
            backupKey++;

        System.out.println("primaryKey = " + primaryKey);
        System.out.println("backupKey = " + backupKey);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    public void testClientSingleRead() throws Exception {
        run(client(), (Ignite ign) -> {
            ign.cache("cache").get(primaryKey);
        }, 1);
    }

    public void testPrimarySingleRead() throws Exception {
        run(primary(), (Ignite ign) -> {
            ign.cache("cache").get(primaryKey);
        }, 1);
    }

    public void testBackupSingleRead() throws Exception {
        run(backup(), (Ignite ign) -> {
            ign.cache("cache").get(primaryKey);
        }, 1);
    }



    public void testClientMultiRead() throws Exception {
        run(client(), (Ignite ign) -> {
            ign.cache("cache").get(primaryKey);
            ign.cache("cache").get(backupKey);
        }, 2);
    }

    public void testPrimaryMultiRead() throws Exception {
        run(primary(), (Ignite ign) -> {
            ign.cache("cache").get(primaryKey);
            ign.cache("cache").get(backupKey);
        }, 2);
    }

    public void testBackupMultiRead() throws Exception {
        run(backup(), (Ignite ign) -> {
            ign.cache("cache").get(primaryKey);
            ign.cache("cache").get(backupKey);
        }, 2);
    }




    public void testClientSingleWrite() throws Exception {
        run(client(), (Ignite ign) -> {
            ign.cache("cache").put(primaryKey, 1);
        }, 2);
    }

    public void testPrimarySingleWrite() throws Exception {
        run(primary(), (Ignite ign) -> {
            ign.cache("cache").put(primaryKey, 1);
        }, 2);
    }

    public void testBackupSingleWrite() throws Exception {
        run(backup(), (Ignite ign) -> {
            ign.cache("cache").put(primaryKey, 1);
        }, 2);
    }


    public void testClientMultiWrite() throws Exception {
        run(client(), (Ignite ign) -> {
            ign.cache("cache").put(primaryKey, 1);
            ign.cache("cache").put(backupKey, 1);
        }, 4);
    }

    public void testPrimaryMultiWrite() throws Exception {
        run(primary(), (Ignite ign) -> {
            ign.cache("cache").put(primaryKey, 1);
            ign.cache("cache").put(backupKey, 1);
        }, 4);
    }

    public void testBackupMultiWrite() throws Exception {
        run(backup(), (Ignite ign) -> {
            ign.cache("cache").put(primaryKey, 1);
            ign.cache("cache").put(backupKey, 1);
        }, 4);
    }

    /**
     * Run command in transaction and check that number of events with required name of transaction equals  expected
     * value
     *
     * @param startNode Ignite node to start transaction and run passed command
     * @param cmdInTx command which should be done in transaction
     * @param expectedEventsCount expected events number with label of transaction
     * @throws Exception If failed.
     */
    public void run(Ignite startNode, Consumer<Ignite> cmdInTx, int expectedEventsCount) throws Exception {
        Ignite primary = primary();
        Ignite backup = backup();
        Ignite client = client();

        AtomicInteger evtCnt = new AtomicInteger();

        eventListener(primary, evtCnt, "PRIM");
        eventListener(backup, evtCnt, "BACK");
        eventListener(client, evtCnt, "CLI");

        try (Transaction tx = startNode.transactions().withLabel(TX_LABEL).txStart(transactionConcurrency(), transactionIsolation())) {
            cmdInTx.accept(startNode);
            tx.commit();
        }

        GridTestUtils.waitForCondition(() ->expectedEventsCount == evtCnt.get(), 2000);

        Assert.assertEquals(expectedEventsCount, evtCnt.get());

    }

    private Ignite primary() {
        return ignite(0);
    }

    private Ignite backup() {
        return ignite(1);
    }

    private Ignite client() {
        return ignite(2);
    }

    protected int backups() {
        return 1;
    }

    protected TransactionConcurrency transactionConcurrency() {
        return OPTIMISTIC;
    }

    protected TransactionIsolation transactionIsolation() {
        return REPEATABLE_READ;
    }

    private void eventListener(Ignite ign, AtomicInteger evtCnt, String instanceName) {
        ign.events().enableLocal(CACHE_EVENT_TYPES);
        ign.events().localListen((IgnitePredicate<Event>)event -> {
            CacheEvent evt = ((CacheEvent)event);
            System.out.println("!!!!! " + instanceName + " -   " + evt.txLabel());
            System.out.println(evt);
            Assert.assertEquals(TX_LABEL, evt.txLabel());
            evtCnt.incrementAndGet();
            return true;
        }, CACHE_EVENT_TYPES);
    }
}
