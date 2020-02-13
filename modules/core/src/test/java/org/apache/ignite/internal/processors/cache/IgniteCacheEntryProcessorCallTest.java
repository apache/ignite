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
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 *
 */
public class IgniteCacheEntryProcessorCallTest extends GridCommonAbstractTest {
    /** */
    static final AtomicInteger callCnt = new AtomicInteger();

    /** */
    private static final int SRV_CNT = 4;

    /** */
    private static final int NODES = 5;

    /** */
    private static final int OP_UPDATE = 1;

    /** */
    private static final int OP_REMOVE = 2;

    /** */
    private static final int OP_GET = 3;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(SRV_CNT);

        Ignite client = startClientGrid(SRV_CNT);

        assertTrue(client.configuration().isClientMode());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEntryProcessorCallOnAtomicCache() throws Exception {
        {
            CacheConfiguration<Integer, TestValue> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);
            ccfg.setBackups(1);
            ccfg.setWriteSynchronizationMode(FULL_SYNC);
            ccfg.setAtomicityMode(ATOMIC);

            checkEntryProcessorCallCount(ccfg, 1);
        }

        {
            CacheConfiguration<Integer, TestValue> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);
            ccfg.setBackups(0);
            ccfg.setWriteSynchronizationMode(FULL_SYNC);
            ccfg.setAtomicityMode(ATOMIC);

            checkEntryProcessorCallCount(ccfg, 1);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEntryProcessorCallOnTxCache() throws Exception {
        {
            CacheConfiguration<Integer, TestValue> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);
            ccfg.setBackups(1);
            ccfg.setWriteSynchronizationMode(FULL_SYNC);
            ccfg.setAtomicityMode(TRANSACTIONAL);

            checkEntryProcessorCallCount(ccfg, 2);
        }

        {
            CacheConfiguration<Integer, TestValue> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);
            ccfg.setBackups(0);
            ccfg.setWriteSynchronizationMode(FULL_SYNC);
            ccfg.setAtomicityMode(TRANSACTIONAL);

            checkEntryProcessorCallCount(ccfg, 1);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEntryProcessorCallOnMvccCache() throws Exception {
        {
            CacheConfiguration<Integer, TestValue> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);
            ccfg.setBackups(1);
            ccfg.setWriteSynchronizationMode(FULL_SYNC);
            ccfg.setAtomicityMode(TRANSACTIONAL_SNAPSHOT);

            checkEntryProcessorCallCount(ccfg, 2);
        }

        {
            CacheConfiguration<Integer, TestValue> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);
            ccfg.setBackups(0);
            ccfg.setWriteSynchronizationMode(FULL_SYNC);
            ccfg.setAtomicityMode(TRANSACTIONAL_SNAPSHOT);

            checkEntryProcessorCallCount(ccfg, 1);
        }
    }

    /**
     * @param ccfg Cache configuration.
     * @param expCallCnt Expected entry processor calls count.
     * @throws Exception If failed.
     */
    private void checkEntryProcessorCallCount(CacheConfiguration<Integer, TestValue> ccfg,
        int expCallCnt) throws Exception {
        Ignite client1 = ignite(SRV_CNT);

        IgniteCache<Integer, TestValue> clientCache1 = client1.createCache(ccfg);

        IgniteCache<Integer, TestValue> srvCache = ignite(0).cache(ccfg.getName());

        awaitPartitionMapExchange();

        int key = 0;

        // Call EntryProcessor on every node to ensure that binary metadata has been registered everywhere.
        for (int i = 0; i < 1_000; i++)
            ignite(i % SRV_CNT).<Integer, TestValue>cache(ccfg.getName())
                .invoke(key++, new TestEntryProcessor(OP_UPDATE), new TestValue(Integer.MIN_VALUE));

        checkEntryProcessCall(key++, clientCache1, null, null, expCallCnt);

        if (ccfg.getAtomicityMode() == TRANSACTIONAL) {
            checkEntryProcessCall(key++, clientCache1, OPTIMISTIC, REPEATABLE_READ, expCallCnt + 1);
            checkEntryProcessCall(key++, clientCache1, OPTIMISTIC, SERIALIZABLE, expCallCnt + 1);
            checkEntryProcessCall(key++, clientCache1, PESSIMISTIC, REPEATABLE_READ, expCallCnt + 1);
        }
        else if (ccfg.getAtomicityMode() == TRANSACTIONAL_SNAPSHOT)
            checkEntryProcessCall(key++, clientCache1, PESSIMISTIC, REPEATABLE_READ, expCallCnt);

        for (int i = 100; i < 110; i++) {
            checkEntryProcessCall(key++, srvCache, null, null, expCallCnt);

            if (ccfg.getAtomicityMode() == TRANSACTIONAL) {
                checkEntryProcessCall(key++, clientCache1, OPTIMISTIC, REPEATABLE_READ, expCallCnt + 1);
                checkEntryProcessCall(key++, clientCache1, OPTIMISTIC, SERIALIZABLE, expCallCnt + 1);
                checkEntryProcessCall(key++, clientCache1, PESSIMISTIC, REPEATABLE_READ, expCallCnt + 1);
            }
            else if (ccfg.getAtomicityMode() == TRANSACTIONAL_SNAPSHOT)
                checkEntryProcessCall(key++, clientCache1, PESSIMISTIC, REPEATABLE_READ, expCallCnt);
        }

        for (int i = 0; i < NODES; i++)
            ignite(i).destroyCache(ccfg.getName());
    }

    /**
     * @param key Key.
     * @param cache Cache.
     * @param concurrency Transaction concurrency.
     * @param isolation Transaction isolation.
     * @param expCallCnt Expected entry processor calls count.
     */
    private void checkEntryProcessCall(Integer key,
        IgniteCache<Integer, TestValue> cache,
        @Nullable TransactionConcurrency concurrency,
        @Nullable TransactionIsolation isolation,
        int expCallCnt) {
        Ignite ignite = cache.unwrap(Ignite.class);

        ClusterNode primary = ignite.affinity(cache.getName()).mapKeyToNode(key);

        assertNotNull(primary);

        log.info("Check call [key=" + key +
            ", primary=" + primary.attribute(ATTR_IGNITE_INSTANCE_NAME) +
            ", concurrency=" + concurrency +
            ", isolation=" + isolation + "]");

        int expCallCntOnGet = cache.getConfiguration(CacheConfiguration.class).getAtomicityMode() == TRANSACTIONAL_SNAPSHOT ?
            1 : expCallCnt;

        Transaction tx;
        TestReturnValue retVal;

        log.info("Invoke: " + key);

        // Update.
        callCnt.set(0);

        tx = startTx(cache, concurrency, isolation);

        retVal = cache.invoke(key, new TestEntryProcessor(OP_UPDATE), new TestValue(Integer.MIN_VALUE));

        if (tx != null)
            tx.commit();

        assertEquals(expCallCnt, callCnt.get());

        checkReturnValue(retVal, "null");
        checkCacheValue(cache.getName(), key, new TestValue(0));

        log.info("Invoke: " + key);

        // Get.
        callCnt.set(0);

        tx = startTx(cache, concurrency, isolation);

        retVal = cache.invoke(key, new TestEntryProcessor(OP_GET), new TestValue(Integer.MIN_VALUE));

        if (tx != null)
            tx.commit();

        assertEquals(expCallCntOnGet, callCnt.get());

        checkReturnValue(retVal, "0");
        checkCacheValue(cache.getName(), key, new TestValue(0));

        log.info("Invoke: " + key);

        // Update.
        callCnt.set(0);

        tx = startTx(cache, concurrency, isolation);

        retVal = cache.invoke(key, new TestEntryProcessor(OP_UPDATE), new TestValue(Integer.MIN_VALUE));

        if (tx != null)
            tx.commit();

        assertEquals(expCallCnt, callCnt.get());

        checkReturnValue(retVal, "0");
        checkCacheValue(cache.getName(), key, new TestValue(1));

        log.info("Invoke: " + key);

        // Remove.
        callCnt.set(0);

        tx = startTx(cache, concurrency, isolation);

        retVal = cache.invoke(key, new TestEntryProcessor(OP_REMOVE), new TestValue(Integer.MIN_VALUE));

        if (tx != null)
            tx.commit();

        assertEquals(expCallCnt, callCnt.get());

        checkReturnValue(retVal, "1");
        checkCacheValue(cache.getName(), key, null);
    }

    /**
     * @param retVal Return value.
     * @param expVal Expected value.
     */
    private void checkReturnValue(TestReturnValue retVal, String expVal) {
        assertNotNull(retVal);

        TestValue arg = (TestValue)retVal.argument();
        assertNotNull(arg);
        assertEquals(Integer.MIN_VALUE, (Object)arg.value());

        assertEquals(expVal, retVal.value());
    }

    /**
     * @param cacheName Cache name.
     * @param key Key.
     * @param expVal Expected value.
     */
    private void checkCacheValue(String cacheName, Integer key, TestValue expVal) {
        for (int i = 0; i < NODES; i++) {
            Ignite ignite = ignite(i);

            IgniteCache<Integer, TestValue> cache = ignite.cache(cacheName);

            assertEquals(expVal, cache.get(key));
        }
    }

    /**
     * @param cache Cache.
     * @param concurrency Transaction concurrency.
     * @param isolation Transaction isolation.
     * @return Started transaction.
     */
    @Nullable private Transaction startTx(IgniteCache<Integer, TestValue> cache,
        @Nullable TransactionConcurrency concurrency,
        @Nullable TransactionIsolation isolation) {
        if (concurrency != null) {
            assert isolation != null;

            return cache.unwrap(Ignite.class).transactions().txStart(concurrency, isolation);
        }

        return null;
    }

    /**
     *
     */
    static class TestEntryProcessor implements EntryProcessor<Integer, TestValue, TestReturnValue> {
        /** */
        private int op;

        /**
         * @param op Operation.
         */
        public TestEntryProcessor(int op) {
            this.op = op;
        }

        /** {@inheritDoc} */
        @Override public TestReturnValue process(MutableEntry<Integer, TestValue> entry,
            Object... args) {
            Ignite ignite = entry.unwrap(Ignite.class);

            ignite.log().info("TestEntryProcessor called [op=" + op + ", entry=" + entry + ']');

            callCnt.incrementAndGet();

            assertEquals(1, args.length);

            TestReturnValue retVal;

            TestValue val = entry.getValue();

            if (val == null)
                retVal = new TestReturnValue("null", args[0]);
            else
                retVal = new TestReturnValue(String.valueOf(val.value()), args[0]);

            switch (op) {
                case OP_GET:
                    return retVal;

                case OP_UPDATE: {
                    if (val == null)
                        val = new TestValue(0);
                    else
                        val = new TestValue(val.val + 1);

                    entry.setValue(val);

                    break;
                }

                case OP_REMOVE:
                    entry.remove();

                    break;

                default:
                    assert false;
            }

            return retVal;
        }
    }

    /**
     *
     */
    static class TestValue {
        /** */
        private Integer val;

        /**
         * @param val Value.
         */
        public TestValue(Integer val) {
            this.val = val;
        }

        /**
         * @return Value.
         */
        public Integer value() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestValue testVal = (TestValue)o;

            return val.equals(testVal.val);

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return val.hashCode();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TestValue.class, this);
        }
    }

    /**
     *
     */
    static class TestReturnValue {
        /** */
        private String val;

        /** */
        private Object arg;

        /**
         * @param val Value.
         * @param arg Entry processor argument.
         */
        public TestReturnValue(String val, Object arg) {
            this.val = val;
            this.arg = arg;
        }

        /**
         * @return Value.
         */
        public String value() {
            return val;
        }

        /**
         * @return Entry processor argument.
         */
        public Object argument() {
            return arg;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestReturnValue testVal = (TestReturnValue)o;

            return val.equals(testVal.val);

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return val.hashCode();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TestReturnValue.class, this);
        }
    }
}
