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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Assert;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_READ;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_REMOVED;

/**
 * Base test to check passing transaction's label for EVT_CACHE_OBJECT_READ, EVT_CACHE_OBJECT_PUT,
 * EVT_CACHE_OBJECT_REMOVED events.
 */
public abstract class CashEventWithTxLabelAbstractTest extends GridCommonAbstractTest {

    /** types event to be checked. */
    private static final int[] CACHE_EVENT_TYPES = {EVT_CACHE_OBJECT_READ, EVT_CACHE_OBJECT_PUT, EVT_CACHE_OBJECT_REMOVED};

    /** Transaction label. */
    private static final String TX_LABEL = "TX_LABEL";

    /** Number of server nodes. */
    private static final int SRVS = 3;

    /** Number of client nodes. */
    private static final int CLIENTS = 1;

    /** Cache name. */
    public static final String CACHE_NAME = "cache";

    /** Client or server mode to start Ignite instance. */
    private boolean client;

    /** Key related to primary node. */
    private Integer primaryKey = 0;

    /** Key related to backup node. */
    private Integer backupKey = 0;

    /** List to keep all events with no tx label between run tests */
    private static List<CacheEvent> incorrectEvts = Collections.synchronizedList(new ArrayList<>());

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClientMode(client);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        client = false;

        startGridsMultiThreaded(SRVS);

        client = true;

        startGridsMultiThreaded(SRVS, CLIENTS);

        client = false;

        waitForDiscovery(primary(), backup1(), backup2(), client());

        registerEventListeners(primary(), backup1(), backup2(), client());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        incorrectEvts.clear();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        Assert.assertTrue("Has been received " + incorrectEvts.size() + " cache events with incorrect txlabel",
            incorrectEvts.isEmpty());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /**
     * Test single read from client node with backup set as 0.
     *
     * @throws Exception If failed.
     */
    public void testClientSingleReadWithNoBackup() throws Exception {
        prepareCache(0);

        singleWriteReadTest(primary(), client());
    }

    /**
     * Test single read from client node with backup set as 1.
     *
     * @throws Exception If failed.
     */
    public void testClientSingleReadWithOneBackup() throws Exception {
        prepareCache(1);

        singleWriteReadTest(primary(), client());
    }

    /**
     * Test single read from client node with backup set as 2.
     *
     * @throws Exception If failed.
     */
    public void testClientSingleReadWithTwoBackups() throws Exception {
        prepareCache(2);

        singleWriteReadTest(primary(), client());
    }

    /**
     * Test single read from primary node with backup set as 0.
     *
     * @throws Exception If failed.
     */
    public void testPrimarySingleReadWithNoBackup() throws Exception {
        prepareCache(0);

        singleWriteReadTest(client(), primary());
    }

    /**
     * Test single read from primary node with backup set as 1.
     *
     * @throws Exception If failed.
     */
    public void testPrimarySingleReadWithOneBackup() throws Exception {
        prepareCache(1);

        singleWriteReadTest(client(), primary());
    }

    /**
     * Test single read from primary node with backup set as 2.
     *
     * @throws Exception If failed.
     */
    public void testPrimarySingleReadWithTwoBackup() throws Exception {
        prepareCache(2);

        singleWriteReadTest(client(), primary());
    }

    /**
     * Test single read from Ignite node passed as parameter.
     *
     * @param instanceToPut Ignite instance to put test data.
     * @param instanceToGet Ignite instance to get test data.
     * @throws Exception If failed.
     */
    private void singleWriteReadTest(Ignite instanceToPut, Ignite instanceToGet) throws Exception {
        runTransactionally(instanceToPut, (Ignite ign) -> {
            ign.cache(CACHE_NAME).put(primaryKey, 3);
        });

        runTransactionally(instanceToGet, (Ignite ign) -> {
            ign.cache(CACHE_NAME).get(primaryKey);
        });
    }

    /**
     * Test single read from backup node with backup set as 1
     *
     * @throws Exception If failed.
     */
    public void testBackupSingleReadWithOneBackup() throws Exception {
        prepareCache(1);

        multiWriteReadTest(primary(), backup1());
    }

    /**
     * Test single read from primary node with backup set as 2.
     *
     * @throws Exception If failed.
     */
    public void testBackupSingleReadWithTwoBackup() throws Exception {
        prepareCache(2);

        multiWriteReadTest(primary(), backup1());
    }

    /**
     * Test multi read from client node with backup set as 0.
     *
     * @throws Exception If failed.
     */
    public void testClientMultiReadWithNoBackup() throws Exception {
        prepareCache(0);

        multiWriteReadTest(primary(), client());
    }

    /**
     * Test multi read from client node with backup set as 1.
     *
     * @throws Exception If failed.
     */
    public void testClientMultiReadWithOneBackup() throws Exception {
        prepareCache(1);

        multiWriteReadTest(primary(), client());
    }

    /**
     * Test multi read from client node with backup set as 2.
     *
     * @throws Exception If failed.
     */
    public void testClientMultiReadWithTwoBackup() throws Exception {
        prepareCache(2);

        multiWriteReadTest(primary(), client());
    }

    /**
     * Test multi read from primary node with backup set as 0.
     *
     * @throws Exception If failed.
     */
    public void testPrimaryMultiReadWithNoBackup() throws Exception {
        prepareCache(0);

        multiWriteReadTest(client(), primary());
    }

    /**
     * Test multi read from primary node with backup set as 1.
     *
     * @throws Exception If failed.
     */
    public void testPrimaryMultiReadWithOneBackup() throws Exception {
        prepareCache(1);

        multiWriteReadTest(client(), primary());
    }

    /**
     * Test multi read from primary node with backup set as 2.
     *
     * @throws Exception If failed.
     */
    public void testPrimaryMultiReadWithTwoBackup() throws Exception {
        prepareCache(2);

        multiWriteReadTest(client(), primary());
    }

    /**
     * Test multi read from backup node with backup set as 1.
     *
     * @throws Exception If failed.
     */
    public void testBackupMultiReadWithOneBackup() throws Exception {
        prepareCache(1);

        multiWriteReadTest(primary(), backup1());
    }

    /**
     * Test multi read from backup node with backup set as 2.
     *
     * @throws Exception If failed.
     */
    public void testBackupMultiReadWithTwoBackup() throws Exception {
        prepareCache(2);

        multiWriteReadTest(primary(), backup1());
    }

    /**
     * Test multi write and read from nodes passed as parameters.
     *
     * @param instanceToPut Ignite instance to put test data.
     * @param instanceToGet Ignite instance to get test data.
     * @throws Exception If failed.
     */
    private void multiWriteReadTest(Ignite instanceToPut, Ignite instanceToGet) throws Exception {
        runTransactionally(instanceToPut, (Ignite ign) -> {
            ign.cache(CACHE_NAME).put(backupKey, 2);
            ign.cache(CACHE_NAME).put(primaryKey, 3);
        });

        runTransactionally(instanceToGet, (Ignite ign) -> {
            ign.cache(CACHE_NAME).get(primaryKey);
            ign.cache(CACHE_NAME).get(backupKey);
        });
    }

    /**
     * Test multi write and remove from client node with backup set as 0
     *
     * @throws Exception If failed.
     */
    public void testClientSingleWriteWithNoBackup() throws Exception {
        prepareCache(0);

        singleWriteAndRemoveTest(client());
    }

    /**
     * Test multi write and remove from client node with backup set as 1.
     *
     * @throws Exception If failed.
     */
    public void testClientSingleWriteWithOneBackup() throws Exception {
        prepareCache(1);

        singleWriteAndRemoveTest(client());
    }

    /**
     * Test multi write and remove from client node with backup set as 2.
     *
     * @throws Exception If failed.
     */
    public void testClientSingleWriteWithTwoBackup() throws Exception {
        prepareCache(2);

        singleWriteAndRemoveTest(client());
    }

    /**
     * Test single write and remove from primary node with backup set as 0.
     *
     * @throws Exception If failed.
     */
    public void testPrimarySingleWriteWithNoBackup() throws Exception {
        prepareCache(0);

        singleWriteAndRemoveTest(primary());
    }

    /**
     * Test single write and remove from primary node with backup set as 1.
     *
     * @throws Exception If failed.
     */
    public void testPrimarySingleWriteWithOneBackup() throws Exception {
        prepareCache(1);

        singleWriteAndRemoveTest(primary());
    }

    /**
     * Test single write and remove from primary node with backup set as 2.
     *
     * @throws Exception If failed.
     */
    public void testPrimarySingleWriteWithTwoBackup() throws Exception {
        prepareCache(2);

        singleWriteAndRemoveTest(primary());
    }

    /**
     * Test single write and remove from backup node with backup set as 1.
     *
     * @throws Exception If failed.
     */
    public void testBackupSingleWriteWithOneBackup() throws Exception {
        prepareCache(1);

        singleWriteAndRemoveTest(backup1());
    }

    /**
     * Test single write and remove from backup node with backup set as 2.
     *
     * @throws Exception If failed.
     */
    public void testBackupSingleWriteWithTwoBackup() throws Exception {
        prepareCache(0);

        singleWriteAndRemoveTest(backup1());
    }

    /**
     * Test single write and remove from backup node.
     *
     * @param instance Ignite instance to put and remove test data.
     * @throws Exception If failed.
     */
    private void singleWriteAndRemoveTest(Ignite instance) throws Exception {
        runTransactionally(instance, (Ignite ign) -> {
            ign.cache(CACHE_NAME).put(primaryKey, 1);
        });

        runTransactionally(instance, (Ignite ign) -> {
            ign.cache(CACHE_NAME).remove(primaryKey);
        });
    }

    /**
     * Test multi write and remove from client node with backup set as 0.
     *
     * @throws Exception If failed.
     */
    public void testClientMultiWriteWithNoBackup() throws Exception {
        prepareCache(0);

        multiWriteAndRemoveTest(client());
    }

    /**
     * Test multi write and remove from client node with backup set as 1.
     *
     * @throws Exception If failed.
     */
    public void testClientMultiWriteWithOneBackup() throws Exception {
        prepareCache(1);

        multiWriteAndRemoveTest(client());
    }

    /**
     * Test multi write and remove from client node with backup set as 2.
     *
     * @throws Exception If failed.
     */
    public void testClientMultiWriteWithTwoBackup() throws Exception {
        prepareCache(2);

        multiWriteAndRemoveTest(client());
    }

    /**
     * Test multi write and remove from primary node with backup set as 0.
     *
     * @throws Exception If failed.
     */
    public void testPrimaryMultiWriteWithNoBackup() throws Exception {
        prepareCache(0);

        multiWriteAndRemoveTest(primary());
    }

    /**
     * Test multi write and remove from primary node with backup set as 1.
     *
     * @throws Exception If failed.
     */
    public void testPrimaryMultiWriteWithOneBackup() throws Exception {
        prepareCache(1);

        multiWriteAndRemoveTest(primary());
    }

    /**
     * Test multi write and remove from primary node with backup set as 2.
     *
     * @throws Exception If failed.
     */
    public void testPrimaryMultiWriteWithTwoBackup() throws Exception {
        prepareCache(2);

        multiWriteAndRemoveTest(primary());
    }

    /**
     * Test multi write and remove from backup node with backup set as 1.
     *
     * @throws Exception If failed.
     */
    public void testBackupMultiWriteWithOneBackup() throws Exception {
        prepareCache(1);

        multiWriteAndRemoveTest(backup1());
    }

    /**
     * Test multi write and remove from backup node with backup set as 2.
     *
     * @throws Exception If failed.
     */
    public void testBackupMultiWriteWithTwoBackup() throws Exception {
        prepareCache(2);

        multiWriteAndRemoveTest(backup1());
    }

    /**
     * Test multi write and remove from passed as parameter Ignite node.
     *
     * @param instance Ignite instance to put and remove test data.
     * @throws Exception If failed.
     */
    private void multiWriteAndRemoveTest(Ignite instance) throws Exception {
        runTransactionally(instance, (Ignite ign) -> {
            ign.cache(CACHE_NAME).put(primaryKey, 1);
            ign.cache(CACHE_NAME).put(backupKey, 1);
        });

        runTransactionally(instance, (Ignite ign) -> {
            ign.cache(CACHE_NAME).remove(primaryKey);
            ign.cache(CACHE_NAME).remove(backupKey);
        });
    }

    /**
     * Test batch write-read from client node with backup set as 0.
     *
     * @throws Exception If failed.
     */
    public void testPrimaryBatchWriteReadRemoveWithNoBackup() throws Exception {
        prepareCache(0);

        batchWriteReadRemoveTest(client(), primary());
    }

    /**
     * Test batch write-read from client node with backup set as 1.
     *
     * @throws Exception If failed.
     */
    public void testPrimaryBatchWriteReadRemoveWithOneBackup() throws Exception {
        prepareCache(1);

        batchWriteReadRemoveTest(client(), primary());
    }

    /**
     * Test batch write-read from client node with backup set as 2.
     *
     * @throws Exception If failed.
     */
    public void testPrimaryBatchWriteReadRemoveTwoBackup() throws Exception {
        prepareCache(2);

        batchWriteReadRemoveTest(client(), primary());
    }

    /**
     * Test batch write-read from client node with backup set as 0.
     *
     * @throws Exception If failed.
     */
    public void testClientBatchWriteReadRemoveWithNoBackup() throws Exception {
        prepareCache(0);

        batchWriteReadRemoveTest(primary(), client());
    }

    /**
     * Test batch write-read from client node with backup set as 1.
     *
     * @throws Exception If failed.
     */
    public void testClientBatchWriteReadRemoveWithOneBackup() throws Exception {
        prepareCache(2);

        batchWriteReadRemoveTest(primary(), client());
    }

    /**
     * Test batch write-read from client node with backup set as 2.
     *
     * @throws Exception If failed.
     */
    public void testClientBatchWriteReadRemoveWithTwoBackup() throws Exception {
        prepareCache(2);

        batchWriteReadRemoveTest(primary(), client());
    }

    /**
     * Test batch write-read from client node.
     *
     * @param instanceToPut Ignite instance to put test data.
     * @param instanceToGet Ignite instance to get test data.
     * @throws Exception If failed.
     */
    private void batchWriteReadRemoveTest(Ignite instanceToPut, Ignite instanceToGet) throws Exception {
        Map<Integer, Integer> keyValuesMap = IntStream.range(0, 100).boxed()
            .collect(Collectors.toMap(Function.identity(), Function.identity()));

        runTransactionally(instanceToPut, (Ignite ign) -> {
            ign.cache(CACHE_NAME).putAll(keyValuesMap);
        });

        runTransactionally(instanceToGet, (Ignite ign) -> {
            ign.cache(CACHE_NAME).getAll(keyValuesMap.keySet());
        });

        runTransactionally(instanceToGet, (Ignite ign) -> {
            ign.cache(CACHE_NAME).removeAll(keyValuesMap.keySet());
        });
    }

    /**
     * Run command in transaction.
     *
     * @param startNode Ignite node to start transaction and run passed command.
     * @param cmdInTx Command which should be done in transaction.
     * @throws Exception If failed.
     */
    private void runTransactionally(Ignite startNode, Consumer<Ignite> cmdInTx) throws Exception {
        try (Transaction tx = startNode.transactions().withLabel(TX_LABEL).txStart(transactionConcurrency(), transactionIsolation())) {
            cmdInTx.accept(startNode);
            tx.commit();
        }
    }

    /**
     * Add event listener to passed Ignite instances for cache event types.
     *
     * @param igns Ignite instances.
     */
    private void registerEventListeners(Ignite... igns) {
        if (igns != null)
            for (Ignite ign : igns) {
                ign.events().enableLocal(CACHE_EVENT_TYPES);
                ign.events().localListen((IgnitePredicate<Event>)event -> {
                    CacheEvent cacheEvt = (CacheEvent)event;

                    if (!TX_LABEL.equals(cacheEvt.txLabel())) {
                        log.error("Has been received event with incorrect label " + cacheEvt.txLabel() + " ," +
                            " expected " + TX_LABEL + " label");

                        incorrectEvts.add(cacheEvt);
                    }

                    return true;
                }, CACHE_EVENT_TYPES);
            }
    }

    /**
     * Create cache with passed number of backups and determinate primary and backup keys. If cache was created before
     * it will be removed before create new one.
     *
     * @param cacheBackups Number of buckups for cache.
     */
    private void prepareCache(int cacheBackups) throws InterruptedException {
        IgniteCache<Object, Object> cache = client().cache(CACHE_NAME);

        if (cache != null)
            cache.destroy();

        client().createCache(
            new CacheConfiguration<Integer, Integer>()
                .setName(CACHE_NAME)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setCacheMode(CacheMode.PARTITIONED)
                .setBackups(cacheBackups)
        );

        awaitPartitionMapExchange();

        evaluateKeys();
    }

    /**
     * Evaluate primary and backup keys.
     */
    private void evaluateKeys() {
        while (!client().affinity(CACHE_NAME).isPrimary(((IgniteKernal)primary()).localNode(), primaryKey))
            primaryKey++;

        while (!client().affinity(CACHE_NAME).isBackup(((IgniteKernal)primary()).localNode(), backupKey)
            && backupKey < 100)
            backupKey++;
    }

    /**
     * Return primary node.
     *
     * @return Primary node.
     */
    private Ignite primary() {
        return ignite(0);
    }

    /**
     * Return first backup node.
     *
     * @return First backup node.
     */
    private Ignite backup1() {
        return ignite(1);
    }

    /**
     * Return second backup node.
     *
     * @return Second backup node.
     */
    private Ignite backup2() {
        return ignite(2);
    }

    /**
     * Return client node.
     *
     * @return Client node.
     */
    private Ignite client() {
        return ignite(3);
    }

    /**
     * Return transaction concurency level.
     *
     * @return Transaction concurency level.
     */
    protected abstract TransactionConcurrency transactionConcurrency();

    /**
     * Return transaction isolation level.
     *
     * @return Transaction isolation level.
     */
    protected abstract TransactionIsolation transactionIsolation();
}
