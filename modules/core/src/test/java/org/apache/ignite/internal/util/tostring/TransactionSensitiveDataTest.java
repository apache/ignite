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

package org.apache.ignite.internal.util.tostring;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareFutureAdapter;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static java.lang.Thread.currentThread;
import static java.util.Objects.nonNull;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_TO_STRING_INCLUDE_SENSITIVE;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.assertNotContains;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;
import static org.apache.ignite.testframework.GridTestUtils.setFieldValue;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Class for checking sensitive data when outputting transactions to the log.
 */
public class TransactionSensitiveDataTest extends GridCommonAbstractTest {
    /** Listener log messages. */
    private static ListeningTestLogger testLog;

    /** Node count. */
    private static final int NODE_COUNT = 2;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        setFieldValue(GridNearTxPrepareFutureAdapter.class, "log", null);
        ((AtomicReference<IgniteLogger>)getFieldValue(GridNearTxPrepareFutureAdapter.class, "logRef")).set(null);

        clearGridToStringClassCache();

        testLog = new ListeningTestLogger(false, log);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        testLog.clearListeners();

        stopAllGrids();

        clearGridToStringClassCache();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setGridLogger(testLog)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setCacheConfiguration(
                new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                    .setAtomicityMode(TRANSACTIONAL)
                    .setBackups(NODE_COUNT)
                    .setAffinity(new RendezvousAffinityFunction(false, 10))
            );
    }

    /**
     * Test for checking the absence of sensitive data in log during an
     * exchange while an active transaction is running.
     *
     * @throws Exception If failed.
     */
    @WithSystemProperty(key = IGNITE_TO_STRING_INCLUDE_SENSITIVE, value = "false")
    @Test
    public void testHideSensitiveDataDuringExchange() throws Exception {
        checkSensitiveDataDuringExchange((logStr, binObjStr) -> assertNotContains(log, logStr, binObjStr));
    }

    /**
     * Test for checking the presence of sensitive data in log during an
     * exchange while an active transaction is running.
     *
     * @throws Exception If failed.
     */
    @WithSystemProperty(key = IGNITE_TO_STRING_INCLUDE_SENSITIVE, value = "true")
    @Test
    public void testShowSensitiveDataDuringExchange() throws Exception {
        checkSensitiveDataDuringExchange((logStr, binObjStr) -> assertContains(log, logStr, binObjStr));
    }

    /**
     * Test for checking the absence of sensitive data in log when node exits
     * during transaction preparation.
     *
     * @throws Exception If failed.
     */
    @WithSystemProperty(key = IGNITE_TO_STRING_INCLUDE_SENSITIVE, value = "false")
    @Test
    public void testHideSensitiveDataDuringNodeLeft() throws Exception {
        checkSensitiveDataDuringNodeLeft((logStr, binObjStr) -> assertNotContains(log, logStr, binObjStr));
    }

    /**
     * Test for checking the presence of sensitive data in log when node exits
     * during transaction preparation.
     *
     * @throws Exception If failed.
     */
    @WithSystemProperty(key = IGNITE_TO_STRING_INCLUDE_SENSITIVE, value = "true")
    @Test
    public void testShowSensitiveDataDuringNodeLeft() throws Exception {
        checkSensitiveDataDuringNodeLeft((logStr, binObjStr) -> assertContains(log, logStr, binObjStr));
    }

    /**
     * Receiving a log message "Partition release future:" during the exchange
     * to check whether or not sensitive data is in printed transactions.
     *
     * @param check Check sensitive data in log message.
     * @throws Exception If failed.
     */
    private void checkSensitiveDataDuringExchange(BiConsumer<String, String> check) throws Exception {
        assert nonNull(check);

        IgniteEx crd = startGrids(NODE_COUNT);

        awaitPartitionMapExchange();

        AtomicReference<String> strToCheckRef = new AtomicReference<>();

        LogListener logLsnr = LogListener.matches(logStr -> {
            if (logStr.contains("Partition release future:") && currentThread().getName().contains(crd.name())) {
                strToCheckRef.set(logStr);

                return true;
            }

            return false;
        }).build();

        testLog.registerListener(logLsnr);

        IgniteCache<Object, Object> cache = crd.getOrCreateCache(DEFAULT_CACHE_NAME).withKeepBinary();

        IgniteBinary binary = crd.binary();

        BinaryObject binKey = binary.toBinary(new Key(0));
        BinaryObject binPerson = binary.toBinary(new Person(1, "name_1"));

        cache.put(binKey, binPerson);

        Transaction tx = crd.transactions().txStart();

        cache.put(binKey, binPerson);

        GridTestUtils.runAsync(() -> {
            logLsnr.check(10 * crd.configuration().getNetworkTimeout());

            tx.commit();

            return null;
        });

        startGrid(NODE_COUNT);

        check.accept(maskIdHash(strToCheckRef.get()), maskIdHash(toStr(binKey, Key.class)));
        check.accept(maskIdHash(strToCheckRef.get()), maskIdHash(toStr(binPerson, Person.class)));
    }

    /**
     * Receiving the “Failed to send message to remote node” and
     * “Received error when future is done” message logs during the node exit
     * when preparing the transaction to check whether or not sensitive data
     * is in the printed transactions.
     *
     * @param check Check sensitive data in log messages.
     * @throws Exception If failed.
     */
    private void checkSensitiveDataDuringNodeLeft(BiConsumer<String, String> check) throws Exception {
        assert nonNull(check);

        startGrids(NODE_COUNT);

        IgniteEx clientNode = startClientGrid(NODE_COUNT);

        awaitPartitionMapExchange();

        AtomicReference<String> strFailedSndRef = new AtomicReference<>();
        AtomicReference<String> strReceivedErrorRef = new AtomicReference<>();

        testLog.registerListener(logStr -> {
            if (logStr.contains("Failed to send message to remote node"))
                strFailedSndRef.set(logStr);
        });

        testLog.registerListener(logStr -> {
            if (logStr.contains("Received error when future is done"))
                strReceivedErrorRef.set(logStr);
        });

        int stopGridId = 0;

        TestRecordingCommunicationSpi.spi(clientNode).closure((clusterNode, message) -> {
            if (GridNearTxPrepareRequest.class.isInstance(message))
                stopGrid(stopGridId);
        });

        String cacheName = DEFAULT_CACHE_NAME;

        IgniteCache<Object, Object> cache = clientNode.getOrCreateCache(cacheName).withKeepBinary();

        IgniteBinary binary = clientNode.binary();

        BinaryObject binKey = binary.toBinary(new Key(primaryKey(grid(stopGridId).cache(cacheName))));
        BinaryObject binPerson = binary.toBinary(new Person(1, "name_1"));

        try (Transaction tx = clientNode.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            cache.put(binKey, binPerson);

            tx.commit();
        }
        catch (Exception ignored) {
            //ignore
        }

        String strFailedSndStr = maskIdHash(strFailedSndRef.get());
        String strReceivedErrorStr = maskIdHash(strReceivedErrorRef.get());

        String binKeyStr = maskIdHash(toStr(binKey, Key.class));
        String binPersonStr = maskIdHash(toStr(binPerson, Person.class));

        check.accept(strFailedSndStr, binKeyStr);
        check.accept(strFailedSndStr, binPersonStr);

        check.accept(strReceivedErrorStr, binKeyStr);
        check.accept(strReceivedErrorStr, binPersonStr);
    }

    /**
     * Removes a idHash from a string.
     *
     * @param s String.
     * @return String without a idHash.
     */
    private String maskIdHash(String s) {
        assert nonNull(s);

        return s.replaceAll("idHash=[0-9]*", "idHash=NO");
    }

    /**
     * Create a string to search for BinaryObject in the log.
     *
     * @param binPerson BinaryObject.
     * @param cls Class of BinaryObject.
     * @return String representation of BinaryObject.
     */
    private String toStr(BinaryObject binPerson, Class<?> cls) {
        assert nonNull(binPerson);
        assert nonNull(cls);

        return binPerson.toString().replace(cls.getName(), cls.getSimpleName());
    }

    /**
     * Key for mapping value in cache.
     */
    static class Key {
        /** Id. */
        int id;

        /**
         * Constructor.
         *
         * @param id Id.
         */
        public Key(int id) {
            this.id = id;
        }
    }

    /**
     * Person class for cache storage.
     */
    static class Person {
        /** Id organization. */
        int orgId;

        /** Person name. */
        String name;

        /**
         * Constructor.
         *
         * @param orgId Id organization.
         * @param name Person name.
         */
        public Person(int orgId, String name) {
            this.orgId = orgId;
            this.name = name;
        }
    }
}
