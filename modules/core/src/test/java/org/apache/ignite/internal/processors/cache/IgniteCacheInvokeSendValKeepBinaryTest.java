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
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/**
 *
 */
public class IgniteCacheInvokeSendValKeepBinaryTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Keys count. */
    private static final int KEYS_CNT = 10;

    /** Keys count. */
    private static final int ITERATION_CNT = 100;

    /** Cache name. */
    private static final String CACHE_NAME = "test-cache-name";

    /** Nodes count. */
    private static final int NODES = 3;

    /** */
    private boolean client;

    /** */
    private static volatile boolean fail;

    /** */
    private static AtomicInteger cntr = new AtomicInteger();

    /** */
    private static boolean readFromBackup = true;

    /** */
    private static UUID clientNodeId;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        GridTestProperties.setProperty(GridTestProperties.MARSH_CLASS_NAME, BinaryMarshaller.class.getName());

        super.beforeTestsStarted();

        startGridsMultiThreaded(getServerNodeCount());

        client = true;

        startGrid(getServerNodeCount());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        fail = false;
        readFromBackup = true;
        clientNodeId = null;

        cntr.set(0);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);
        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        cfg.setClientMode(client);

        return cfg;
    }

    /**
     * @return Count nodes.
     */
    protected int getServerNodeCount() {
        return NODES;
    }

    /**
     * @throws Exception If failed.
     */
    public void testTwoBackupKeepBinary() throws Exception {
        doTestInvokeKeepBinary(new CacheConfiguration<Integer, Integer>()
            .setBackups(2)
            .setName(CACHE_NAME)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setAtomicityMode(TRANSACTIONAL)
        );
    }

    /**
     * @throws Exception If failed.
     */
    private void doTestInvokeKeepBinary(CacheConfiguration<Integer, Integer> ccfg) throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).createCache(ccfg);

        try {
            invokeKeepBinary(cache, null, false, false);

            invokeKeepBinary(cache, null, true, false);

            invokeKeepBinary(cache, null, false, true);

            invokeKeepBinary(cache, PESSIMISTIC, false, false);

            invokeKeepBinary(cache, PESSIMISTIC, true, false);

            invokeKeepBinary(cache, PESSIMISTIC, false, true);

            invokeKeepBinary(cache, OPTIMISTIC, false, false);

            invokeKeepBinary(cache, OPTIMISTIC, true, false);

            invokeKeepBinary(cache, OPTIMISTIC, false, true);
        }
        catch (Exception e) {
            log.error("Test failed. ", e);

            fail("Test failed.");
        }
        finally {
            for (int i = 0; i <= NODES; i++) {
                Transaction tx = grid(i).transactions().tx();

                if (tx != null)
                    tx.close();
            }

            grid(0).destroyCache(ccfg.getName());
        }
    }

    /**
     * @param cache0 Cache.
     * @param txMode Not null transaction concurrency mode if explicit transaction should be started.
     * @param client Operations will be performed from client node.
     * @throws Exception If failed.
     */
    private void invokeKeepBinary(IgniteCache<Integer, Integer> cache0, @Nullable TransactionConcurrency txMode,
        boolean client, boolean primary) throws Exception {
        Set<Integer> keys = new LinkedHashSet<>();

        for (int i = 0; i < NODES; i++) {
            ClusterNode node = grid(i).cluster().localNode();

            for (int key = 0; key < KEYS_CNT; key++) {
                if (affinity(cache0).isPrimary(node, key))
                    keys.add(key);
            }
        }

        for (final Integer key : keys) {
            cntr.set(0);

            log.info("Test invoke [key=" + key + ", txMode=" + txMode + ']');

            Ignite ignite = getAffinityOrClientNode(cache0, client, primary, key);

            IgniteCache<Integer, TestValue2> cache = ignite.<Integer, TestValue2>cache(cache0.getName())
                .withSendValueToBackup();

            IgniteCache<Integer, BinaryObject> binaryCache = cache.withKeepBinary();

            cache.remove(key);

            KeepBinaryEntryProcessor processor = new KeepBinaryEntryProcessor(null);

            Transaction tx = startTx(ignite, txMode);

            BinaryObject res = (BinaryObject)binaryCache.invoke(key, processor, 1, 2, 3);

            if (tx != null)
                tx.commit();

            assertNull(res);

            checkValue(key, null);

            assertEquals(1, cntr.get());

            // Put.
            cache.put(key, new TestValue2(1));

            processor = new KeepBinaryEntryProcessor(1);

            tx = startTx(ignite, txMode);

            res = (BinaryObject)binaryCache.invoke(key, processor, 1, 2, 3);

            if (tx != null)
                tx.commit();

            assertTrue(res.hasField(TestValue2.VAL));
            assertEquals(1, res.field(TestValue2.VAL));

            checkValue(key, new TestValue2(2));

            assertEquals(2, cntr.get());

            assertFalse("Entry processor was invoked from backup", fail);
        }

        // Put all \ invoke all.

        cntr.set(0);

        log.info("Test invoke [key=" + Arrays.toString(keys.toArray()) + ", txMode=" + txMode + ']');

        Ignite ignite = grid(0);

        IgniteCache<Integer, TestValue2> cache = ignite.<Integer, TestValue2>cache(cache0.getName()).withSendValueToBackup();

        IgniteCache<Integer, BinaryObject> binaryCache = cache.withKeepBinary();

        cache.removeAll(keys);

        KeepBinaryEntryProcessor processor = new KeepBinaryEntryProcessor(null);

        Transaction tx = startTx(ignite, txMode);

        Map res = (Map)binaryCache.invokeAll(keys, processor, 1, 2, 3);

        if (tx != null)
            tx.commit();

        assertTrue(res.isEmpty());

        checkValue(keys, null);

        assertEquals(keys.size(), cntr.get());

        for (Integer key : keys)
            cache.put(key, new TestValue2(1));

        processor = new KeepBinaryEntryProcessor(1);

        tx = startTx(ignite, txMode);

        res = (Map)binaryCache.invokeAll(keys, processor, 1, 2, 3);

        if (tx != null)
            tx.commit();

        assertEquals(keys.size() * 2, cntr.get());

        for (Object o : res.entrySet()) {
            Object bObj = ((EntryProcessorResult)((Map.Entry)o).getValue()).get();

            assertNotNull(bObj);
            assertTrue(bObj instanceof BinaryObject);

            BinaryObject bObj0 = (BinaryObject)bObj;

            assertTrue(bObj0.hasField(TestValue2.VAL));
            assertEquals(1, bObj0.field(TestValue2.VAL));
        }

        for (Integer key : keys)
            checkValue(key, new TestValue2(2));

        assertFalse("Entry processor was invoked from backup", fail);
    }

    /**
     * @param cache0 Cache.
     * @param client Need client node.
     * @param primary Need primary node.
     * @param key Key.
     * @return Ignite.
     */
    private Ignite getAffinityOrClientNode(IgniteCache<Integer, Integer> cache0, boolean client, boolean primary,
        Integer key) {
        Ignite ignite = null;
        boolean backup = !primary && !client;

        if (client)
            ignite = grid(NODES);
        else {
            for (int i = 0; i < NODES; i++) {
                ClusterNode node = grid(i).cluster().localNode();

                if (primary && affinity(cache0).isPrimary(node, key)) {
                    ignite = grid(i);

                    break;
                }
                else if (backup && affinity(cache0).isBackup(node, key)) {
                    ignite = grid(i);

                    break;
                }
            }
        }

        assert ignite != null;

        return ignite;
    }

    /**
     * @param txMode Transaction mode.
     * @param keys Keys.
     * @param cache Cache.
     * @throws Exception If failed.
     */
    private void checkValues(@Nullable TransactionConcurrency txMode, Set<Integer> keys,
        IgniteCache<Integer, Integer> cache, Integer val, boolean checkOnAllNodes) throws Exception {
        for (Integer key : keys) {
            if (txMode != null && !checkOnAllNodes)
                assertEquals(val, cache.get(key));
            else
                checkValue(key, val);
        }
    }

    /**
     * @param key Key.
     * @param expVal Expected value.
     */
    protected void checkValue(final Object key, @Nullable final Object expVal) throws Exception {
        if (expVal != null) {
            for (int i = 0; i < getServerNodeCount(); i++) {
                final IgniteCache<Object, Object> cache = grid(i).cache(CACHE_NAME);

                GridTestUtils.waitForCondition(new PA() {
                    @Override public boolean apply() {
                        Object o = cache.localPeek(key, CachePeekMode.ONHEAP);

                        return o != null && o.equals(expVal);
                    }
                }, 100L);

                Object val = cache.localPeek(key, CachePeekMode.ONHEAP);

                if (val == null)
                    assertFalse(ignite(0).affinity(CACHE_NAME).isPrimaryOrBackup(ignite(i).cluster().localNode(), key));
                else
                    assertEquals("Unexpected value for grid " + i, expVal, val);
            }
        }
        else {
            for (int i = 0; i < getServerNodeCount(); i++) {
                final IgniteCache<Object, Object> cache = grid(i).cache(CACHE_NAME);

                GridTestUtils.waitForCondition(new PA() {
                    @Override public boolean apply() {
                        return cache.localPeek(key, CachePeekMode.ONHEAP) == null;
                    }
                }, 100L);

                assertNull("Unexpected non null value for grid " + i, cache.localPeek(key, CachePeekMode.ONHEAP));
            }
        }
    }

    /**
     * @param txMode Transaction concurrency mode.
     * @return Transaction.
     */
    @Nullable private Transaction startTx(Ignite ignite, @Nullable TransactionConcurrency txMode) {
        return txMode == null ? null : ignite.transactions().txStart(txMode, READ_COMMITTED);
    }

    /**
     *
     */
    private static class ArgumentsSumProcessor extends AbstractEntryProcessor<Integer, Integer, Integer> {
        /** {@inheritDoc} */
        @Override public Integer process(MutableEntry<Integer, Integer> e, Object... args)
            throws EntryProcessorException {
            super.process(e, args);

            assertNotNull(args);
            assertEquals(3, args.length);
            assertEquals(10, args[0]);
            assertEquals(20, args[1]);
            assertEquals(30, args[2]);

            assertTrue(e.exists());

            Integer res = e.getValue();

            for (Object arg : args)
                res += (Integer)arg;

            e.setValue(res);

            return args.length;
        }
    }

    /**
     *
     */
    protected static class ToStringProcessor extends AbstractEntryProcessor<Integer, Integer, String> {
        /** {@inheritDoc} */
        @Override public String process(MutableEntry<Integer, Integer> e, Object... args)
            throws EntryProcessorException {
            super.process(e, args);

            return String.valueOf(e.getValue());
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ToStringProcessor.class, this);
        }
    }

    /**
     *
     */
    protected static class UserClassValueProcessor extends AbstractEntryProcessor<Integer, Integer, TestValue> {
        /** {@inheritDoc} */
        @Override public TestValue process(MutableEntry<Integer, Integer> e, Object... args)
            throws EntryProcessorException {
            super.process(e, args);

            return new TestValue(String.valueOf(e.getValue()));
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(UserClassValueProcessor.class, this);
        }
    }

    /**
     *
     */
    protected static class CollectionReturnProcessor
        extends AbstractEntryProcessor<Integer, Integer, Collection<TestValue>> {
        /** {@inheritDoc} */
        @Override public Collection<TestValue> process(MutableEntry<Integer, Integer> e, Object... args)
            throws EntryProcessorException {
            super.process(e, args);

            List<TestValue> vals = new ArrayList<>();

            for (int i = 0; i < 10; i++)
                vals.add(new TestValue(String.valueOf(e.getValue() + 1)));

            return vals;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(CollectionReturnProcessor.class, this);
        }
    }

    /**
     *
     */
    protected static class IncrementProcessor extends AbstractEntryProcessor<Integer, Integer, Integer> {
        /** {@inheritDoc} */
        @Override public Integer process(MutableEntry<Integer, Integer> e, Object... args)
            throws EntryProcessorException {
            super.process(e, args);
            if (e.exists()) {
                Integer val = e.getValue();

                assertNotNull(val);

                e.setValue(val + 1);

                assertTrue(e.exists());

                assertEquals(val + 1, (int) e.getValue());

                return val;
            }
            else {
                e.setValue(1);

                return -1;
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(IncrementProcessor.class, this);
        }
    }

    /**
     *
     */
    private static class RemoveProcessor extends AbstractEntryProcessor<Integer, Integer, Integer> {
        /** */
        private Integer expVal;

        /**
         * @param expVal Expected value.
         */
        RemoveProcessor(@Nullable Integer expVal) {
            this.expVal = expVal;
        }

        /** {@inheritDoc} */
        @Override public Integer process(MutableEntry<Integer, Integer> e,
            Object... args) throws EntryProcessorException {
            super.process(e, args);

            assertTrue(e.exists());

            if (expVal != null)
                assertEquals(expVal, e.getValue());

            e.remove();

            assertFalse(e.exists());

            return null;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(RemoveProcessor.class, this);
        }
    }

    /**
     *
     */
    private static class KeepBinaryEntryProcessor
        extends AbstractEntryProcessor<Integer, BinaryObject, Object> {
        /** */
        private Integer expVal;

        /**
         * @param expVal Expected value.
         */
        KeepBinaryEntryProcessor(@Nullable Integer expVal) {
            this.expVal = expVal;
        }

        /** {@inheritDoc} */
        @Override public Object process(MutableEntry<Integer, BinaryObject> e,
            Object... args) throws EntryProcessorException {
            super.process(e, args);

            assertEquals(3, args.length);

            BinaryObject val = e.getValue();

            if (expVal == null)
                assertFalse(e.exists());
            else {
                assertTrue(val.hasField(TestValue2.VAL));
                assertEquals(expVal, val.field(TestValue2.VAL));

                int newVal = expVal + 1;

                e.setValue(val.toBuilder()
                    .setField(TestValue2.VAL, newVal)
                    .hashCode(newVal)
                    .build()
                );
            }

            return val;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(KeepBinaryEntryProcessor.class, this);
        }
    }

    /**
     *
     */
    private static class AbstractEntryProcessor<K, V, T> implements EntryProcessor<K, V, T> {
        /** {@inheritDoc} */
        @Override public T process(MutableEntry<K, V> entry, Object... args) throws EntryProcessorException {
            Ignite ignite = entry.unwrap(Ignite.class);

            cntr.incrementAndGet();

            boolean isPrimary = ignite.affinity(CACHE_NAME).isPrimary(ignite.cluster().localNode(), entry.getKey());
            boolean isBackup = ignite.affinity(CACHE_NAME).isBackup(ignite.cluster().localNode(), entry.getKey());
            boolean isClient = clientNodeId != null && ignite.cluster().localNode().id().equals(clientNodeId);

            if (!isPrimary && !isClient && !(isBackup && readFromBackup)) {
                ignite.log().error("Failed. Entry processor invoked non-primary node: " + ignite.cluster().localNode());

                fail = true;
            }

            return null;
        }
    }

    /**
     *
     */
    static class TestValue2 {
        /** */
        static String VAL = "val";

        /** */
        private Integer val;

        /**
         * @param val Value.
         */
        public TestValue2(Integer val) {
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

            TestValue2 testVal = (TestValue2)o;

            return val.equals(testVal.val);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return val.hashCode();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TestValue2.class, this);
        }
    }

    /**
     *
     */
    static class TestValue {
        /** */
        private String val;

        /**
         * @param val Value.
         */
        public TestValue(String val) {
            this.val = val;
        }

        /**
         * @return Value.
         */
        public String value() {
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
}