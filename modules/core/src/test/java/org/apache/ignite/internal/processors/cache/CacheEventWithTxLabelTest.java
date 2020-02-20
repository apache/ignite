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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_READ;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_REMOVED;

/**
 * Test to check passing transaction's label for EVT_CACHE_OBJECT_READ, EVT_CACHE_OBJECT_PUT,
 * EVT_CACHE_OBJECT_REMOVED events.
 */
public class CacheEventWithTxLabelTest extends GridCommonAbstractTest {
    /** Types event to be checked. */
    private static final int[] CACHE_EVENT_TYPES = {EVT_CACHE_OBJECT_READ, EVT_CACHE_OBJECT_PUT, EVT_CACHE_OBJECT_REMOVED};

    /** Transaction label. */
    private static final String TX_LABEL = "TX_LABEL";

    /** Number of server nodes. */
    private static final int SRVS = 3;

    /** Number of client nodes. */
    private static final int CLIENTS = 1;

    /** Cache name. */
    public static final String CACHE_NAME = "cache";

    /** Key related to primary node. */
    private Integer primaryKey = 0;

    /** Key related to backup node. */
    private Integer backupKey = 0;

    /** Current cash backup count. */
    private int backupCnt;

    /** Current transaction isolation level. */
    private TransactionIsolation isolation;

    /** Current transaction concurrency level. */
    private TransactionConcurrency concurrency;

    /** All failed tests information, */
    private ArrayList<String> errors = new ArrayList<>();

    /** Count of errors on previous iteration of testing. */
    private int prevErrCnt = 0;

    /** List to keep all events with no tx label between run tests */
    private static List<CacheEvent> wrongEvts = Collections.synchronizedList(new ArrayList<>());

    /** Simple entry processor to use for tests */
    private static CacheEntryProcessor entryProcessor = (CacheEntryProcessor)(entry, objects) -> entry.getValue();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(SRVS);
        startClientGridsMultiThreaded(SRVS, CLIENTS);

        waitForDiscovery(primary(), backup1(), backup2(), client());

        registerEventListeners(primary(), backup1(), backup2(), client());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        Assume.assumeFalse("https://issues.apache.org/jira/browse/IGNITE-10270", MvccFeatureChecker.forcedMvcc());
    }

    /**
     * Check all cases for passing transaction label in cash event.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPassTxLabelInCashEventForAllCases() throws Exception {
        Ignite[] nodes = {client(), primary(), backup1(), backup2()};

        for (int backupCnt = 0; backupCnt < SRVS; backupCnt++) {
            this.backupCnt = backupCnt;

            prepareCache(backupCnt);

            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                this.isolation = isolation;

                for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
                    this.concurrency = concurrency;

                    if (MvccFeatureChecker.forcedMvcc() && !MvccFeatureChecker.isSupported(concurrency, isolation))
                        continue;

                    for (int i = 0; i < nodes.length - 1; i++) {
                        Ignite nodeForPut = nodes[i];
                        Ignite nodeForGet = nodes[i + 1];

                        singleWriteReadRemoveTest(nodeForPut, nodeForGet);

                        multiWriteReadRemoveTest(nodeForPut, nodeForGet);

                        singleNodeBatchWriteReadRemoveTest(nodeForPut, nodeForGet);

                        multiNodeBatchWriteReadRemoveTest(nodeForPut, nodeForGet);

                        writeInvokeRemoveTest(nodeForPut, nodeForGet);

                        writeInvokeAllRemoveTest(nodeForPut, nodeForGet);
                    }
                }
            }
        }

        String listOfFailedTests = String.join(",\n", errors);

        Assert.assertTrue("Have been received " + prevErrCnt + " cache events with incorrect txlabel.\n" +
                "Failed tests:" + listOfFailedTests,
            errors.isEmpty());
    }

    /**
     * Check error after run test. In case error occured information about failed test will be added to errors list.
     *
     * @param testName Name of test which result will be checked.
     * @param node1 First node
     * @param node2 Second node
     */
    private void checkResult(String testName, Ignite node1, Ignite node2) {
        int currErrCnt = wrongEvts.size();

        if (prevErrCnt != currErrCnt) {
            prevErrCnt = currErrCnt;

            errors.add(String.format("%s backCnt-%s, %s, %s, node1-%s, node2-%s",
                testName, backupCnt, isolation, concurrency, nodeType(node1), nodeType(node2)));
        }
    }

    /**
     * @param node Ignite node
     * @return Node type in the test
     */
    private String nodeType(Ignite node) {
        if (client().equals(node))
            return "CLIENT";
        else if (primary().equals(node))
            return "PRIMARY";
        else if (backup1().equals(node))
            return "BACKUP1";
        else if (backup2().equals(node))
            return "BACKUP2";
        else
            return "UNKNOWN";
    }

    /**
     * Test single put, get, remove operations.
     *
     * @param instanceToPut Ignite instance to put test data.
     * @param instanceToGet Ignite instance to get test data.
     */
    private void singleWriteReadRemoveTest(Ignite instanceToPut, Ignite instanceToGet) {
        runTransactionally(instanceToPut, (Ignite ign) -> {
            ign.cache(CACHE_NAME).put(primaryKey, 3);
        });

        runTransactionally(instanceToGet, (Ignite ign) -> {
            ign.cache(CACHE_NAME).get(primaryKey);
        });

        runTransactionally(instanceToGet, (Ignite ign) -> {
            ign.cache(CACHE_NAME).remove(primaryKey);
        });

        checkResult("singleWriteReadRemoveTest", instanceToPut, instanceToGet);
    }

    /**
     * Test multi put, get, remove operations
     *
     * @param instanceToPut Ignite instance to put test data.
     * @param instanceToGet Ignite instance to get test data.
     */
    private void multiWriteReadRemoveTest(Ignite instanceToPut, Ignite instanceToGet) {
        runTransactionally(instanceToPut, (Ignite ign) -> {
            ign.cache(CACHE_NAME).put(primaryKey, 2);
            ign.cache(CACHE_NAME).put(backupKey, 3);
        });

        runTransactionally(instanceToGet, (Ignite ign) -> {
            ign.cache(CACHE_NAME).get(primaryKey);
            ign.cache(CACHE_NAME).get(backupKey);
        });

        runTransactionally(instanceToGet, (Ignite ign) -> {
            ign.cache(CACHE_NAME).remove(primaryKey);
            ign.cache(CACHE_NAME).remove(backupKey);
        });

        checkResult("multiWriteReadRemoveTest", instanceToPut, instanceToGet);
    }

    /**
     * Test multi nodes batch write-read
     *
     * @param instanceToPut Ignite instance to put test data.
     * @param instanceToGet Ignite instance to get test data.
     */
    private void multiNodeBatchWriteReadRemoveTest(Ignite instanceToPut, Ignite instanceToGet) {
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

        checkResult("multiNodeBatchWriteReadRemoveTest", instanceToPut, instanceToGet);
    }

    /**
     * Test single node batch write-read-remove
     *
     * @param instanceToPut Ignite instance to put test data.
     * @param instanceToGet Ignite instance to get test data.
     */
    private void singleNodeBatchWriteReadRemoveTest(Ignite instanceToPut, Ignite instanceToGet) {
        IgnitePair<Integer> keys = evaluatePrimaryAndBackupKeys(primaryKey + 1, backupKey + 1);

        Map<Integer, Integer> keyValuesMap = new HashMap<>();
        keyValuesMap.put(primaryKey, 1);
        keyValuesMap.put(keys.get1(), 2);

        runTransactionally(instanceToPut, (Ignite ign) -> {
            ign.cache(CACHE_NAME).putAll(keyValuesMap);
        });

        runTransactionally(instanceToGet, (Ignite ign) -> {
            ign.cache(CACHE_NAME).getAll(keyValuesMap.keySet());
        });

        runTransactionally(instanceToGet, (Ignite ign) -> {
            ign.cache(CACHE_NAME).removeAll(keyValuesMap.keySet());
        });

        checkResult("oneNodeBatchWriteReadRemoveTest", instanceToPut, instanceToGet);
    }

    /**
     * Test put-invoke-remove
     *
     * @param instanceToPut Ignite instance to put test data.
     * @param instanceToGet Ignite instance to get test data.
     */
    @SuppressWarnings("unchecked")
    private void writeInvokeRemoveTest(Ignite instanceToPut, Ignite instanceToGet) {
        runTransactionally(instanceToGet, (Ignite ign) -> {
            ign.cache(CACHE_NAME).put(primaryKey, 3);
        });

        runTransactionally(instanceToPut, (Ignite ign) -> {
            ign.cache(CACHE_NAME).invoke(primaryKey, entryProcessor);
            ign.cache(CACHE_NAME).invoke(backupKey, entryProcessor);
        });

        runTransactionally(instanceToGet, (Ignite ign) -> {
            ign.cache(CACHE_NAME).remove(primaryKey);
        });

        checkResult("writeInvokeRemoveTest", instanceToPut, instanceToGet);
    }

    /**
     * Test putAll-invokeAll-removeAll
     *
     * @param instanceToPut Ignite instance to put test data.
     * @param instanceToGet Ignite instance to get test data.
     */
    @SuppressWarnings("unchecked")
    private void writeInvokeAllRemoveTest(Ignite instanceToPut, Ignite instanceToGet) {
        Map<Integer, Integer> keyValuesMap = IntStream.range(0, 100).boxed()
            .collect(Collectors.toMap(Function.identity(), Function.identity()));

        runTransactionally(instanceToGet, (Ignite ign) -> {
            ign.cache(CACHE_NAME).putAll(keyValuesMap);
        });

        runTransactionally(instanceToPut, (Ignite ign) -> {
            ign.cache(CACHE_NAME).invokeAll(keyValuesMap.keySet(), entryProcessor);
        });

        runTransactionally(instanceToGet, (Ignite ign) -> {
            ign.cache(CACHE_NAME).removeAll(keyValuesMap.keySet());
        });

        checkResult("WriteInvokeAllRemoveTest", instanceToPut, instanceToGet);
    }

    /**
     * Run command in transaction.
     *
     * @param startNode Ignite node to start transaction and run passed command.
     * @param cmdInTx Command which should be done in transaction.
     */
    private void runTransactionally(Ignite startNode, Consumer<Ignite> cmdInTx) {
        try (Transaction tx = startNode.transactions().withLabel(TX_LABEL).txStart(concurrency, isolation)) {
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
        if (igns != null) {
            for (Ignite ign : igns) {
                ign.events().enableLocal(CACHE_EVENT_TYPES);
                ign.events().localListen((IgnitePredicate<Event>)event -> {
                    CacheEvent cacheEvt = (CacheEvent)event;

                    if (!TX_LABEL.equals(cacheEvt.txLabel())) {
                        log.error("Has been received event with incorrect label " + cacheEvt.txLabel() + " ," +
                            " expected " + TX_LABEL + " label");

                        wrongEvts.add(cacheEvt);
                    }

                    return true;
                }, CACHE_EVENT_TYPES);
            }
        }
    }

    /**
     * Create cache with passed number of backups and determinate primary and backup keys. If cache was created before
     * it will be removed before create new one.
     *
     * @param cacheBackups Number of backups for cache.
     * @throws InterruptedException In case of fail.
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

        IgnitePair<Integer> keys = evaluatePrimaryAndBackupKeys(0, 0);

        primaryKey = keys.get1();
        backupKey = keys.get2();
    }

    /**
     * Evaluate primary and backup keys.
     *
     * @param primaryKeyStart Value from need to start calculate primary key.
     * @param backupKeyStart Value from need to start calculate backup key.
     * @return Pair of result. The first result is found primary key. The second is found backup key.
     */
    private IgnitePair<Integer> evaluatePrimaryAndBackupKeys(final int primaryKeyStart, final int backupKeyStart) {
        int primaryKey = primaryKeyStart;
        int backupKey = backupKeyStart;

        while (!client().affinity(CACHE_NAME).isPrimary(((IgniteKernal)primary()).localNode(), primaryKey))
            primaryKey++;

        while (!client().affinity(CACHE_NAME).isBackup(((IgniteKernal)primary()).localNode(), backupKey)
            && backupKey < 100 + backupKeyStart)
            backupKey++;

        return new IgnitePair<>(primaryKey, backupKey);
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
}
