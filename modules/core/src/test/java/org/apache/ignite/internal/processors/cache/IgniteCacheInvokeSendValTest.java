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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class IgniteCacheInvokeSendValTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private Integer lastKey = 0;

    /** */
    private static final int NODES = 3;

    /** */
    private boolean client;

    /** */
    private static volatile boolean fail;

    /** */
    private static AtomicInteger cntr = new AtomicInteger();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
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
     * @return Client node index.
     */
    private int getClientIndex() {
        return getServerNodeCount();
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
    public void testOnePhaseTx() throws Exception {
        doTestInvoke(new CacheConfiguration<Integer, Integer>()
            .setBackups(1)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setAtomicityMode(TRANSACTIONAL)
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testOnePhaseTxPrimarySync() throws Exception {
        doTestInvoke(new CacheConfiguration<Integer, Integer>()
            .setBackups(1)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC)
            .setAtomicityMode(TRANSACTIONAL)
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testTwoBackup() throws Exception {
        doTestInvoke(new CacheConfiguration<Integer, Integer>()
            .setBackups(2)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setAtomicityMode(TRANSACTIONAL)
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testTwoBackupChainInvoke() throws Exception {
        doTestInvokeChain(new CacheConfiguration<Integer, Integer>()
            .setBackups(2)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setAtomicityMode(TRANSACTIONAL)
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testThreeBackup() throws Exception {
        doTestInvoke(new CacheConfiguration<Integer, Integer>()
            .setBackups(3)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setAtomicityMode(TRANSACTIONAL)
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testTwoBackupPrimarySync() throws Exception {
        doTestInvoke(new CacheConfiguration<Integer, Integer>()
            .setBackups(2)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC)
            .setAtomicityMode(TRANSACTIONAL)
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testThreeBackupPrimarySync() throws Exception {
        doTestInvoke(new CacheConfiguration<Integer, Integer>()
            .setBackups(3)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC)
            .setAtomicityMode(TRANSACTIONAL)
        );
    }

    /**
     * @throws Exception If failed.
     */
    private void doTestInvoke(CacheConfiguration<Integer, Integer> ccfg) throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).createCache(ccfg);

        try {
            invoke(cache, null, false, false);

            invoke(cache, null, true, false);

            invoke(cache, null, false, true);

            invoke(cache, PESSIMISTIC, false, false);

            invoke(cache, PESSIMISTIC, true, false);

            invoke(cache, PESSIMISTIC, false, true);

            invoke(cache, OPTIMISTIC, false, false);

            invoke(cache, OPTIMISTIC, true, false);

            invoke(cache, OPTIMISTIC, false, true);
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
     * @throws Exception If failed.
     */
    private void doTestInvokeChain(CacheConfiguration<Integer, Integer> ccfg) throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).createCache(ccfg);

        try {
            invokeChain(cache, null, false, false);

            invokeChain(cache, null, true, false);

            invokeChain(cache, null, false, true);

            invokeChain(cache, PESSIMISTIC, false, false);

            invokeChain(cache, PESSIMISTIC, true, false);

            invokeChain(cache, PESSIMISTIC, false, true);

            invokeChain(cache, OPTIMISTIC, false, false);

            invokeChain(cache, OPTIMISTIC, true, false);

            invokeChain(cache, OPTIMISTIC, false, true);
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

    /** {@inheritDoc} */
    @Override public boolean isDebug() {
        return true;
    }

    /**
     * @param cache0 Cache.
     * @param txMode Not null transaction concurrency mode if explicit transaction should be started.
     * @param client Operations will be performed from client node.
     * @throws Exception If failed.
     */
    private void invoke(IgniteCache<Integer, Integer> cache0, @Nullable TransactionConcurrency txMode, boolean client,
        boolean primary)
        throws Exception {
        IncrementProcessor incProcessor = new IncrementProcessor();

        Ignite ignite = client ? grid(getClientIndex()) :
            (primary ? cache0.unwrap(Ignite.class) : grid(1));

        IgniteCache<Integer, Integer> cache = ignite.cache(cache0.getName());

        cache = cache.withSendValueToBackup();

        for (final Integer key : keys(cache0)) {
            cntr.set(0);

            log.info("Test invoke [key=" + key + ", txMode=" + txMode + ']');

            boolean primary0 = ignite.affinity(null).isPrimary(ignite.cluster().localNode(), key);

            assert (primary && primary0) || (!primary && !primary0);

            cache.remove(key);

            Transaction tx = startTx(ignite, txMode);

            Integer res = cache.invoke(key, incProcessor);

            if (tx != null)
                tx.commit();

            assertEquals(-1, (int)res);
            assertEquals(1, cntr.get());

            checkValue(key, 1);

            tx = startTx(ignite, txMode);

            res = cache.invoke(key, incProcessor);

            if (tx != null)
                tx.commit();

            assertEquals(1, (int)res);
            assertEquals(2, cntr.get());

            checkValue(key, 2);

            tx = startTx(ignite, txMode);

            res = cache.invoke(key, incProcessor);

            if (tx != null)
                tx.commit();

            assertEquals(2, (int)res);
            assertEquals(3, cntr.get());

            checkValue(key, 3);

            tx = startTx(ignite, txMode);

            res = cache.invoke(key, new ArgumentsSumProcessor(), 10, 20, 30);

            if (tx != null)
                tx.commit();

            assertEquals(3, (int)res);
            assertEquals(4, cntr.get());

            checkValue(key, 63);

            tx = startTx(ignite, txMode);

            String strRes = cache.invoke(key, new ToStringProcessor());

            if (tx != null)
                tx.commit();

            assertEquals("63", strRes);
            assertEquals(5, cntr.get());

            checkValue(key, 63);

            tx = startTx(ignite, txMode);

            TestValue testVal = cache.invoke(key, new UserClassValueProcessor());

            if (tx != null)
                tx.commit();

            assertEquals("63", testVal.value());
            assertEquals(6, cntr.get());

            checkValue(key, 63);

            tx = startTx(ignite, txMode);

            Collection<TestValue> testValCol = cache.invoke(key, new CollectionReturnProcessor());

            if (tx != null)
                tx.commit();

            assertEquals(10, testValCol.size());
            assertEquals(7, cntr.get());

            for (TestValue val : testValCol)
                assertEquals("64", val.value());

            checkValue(key, 63);

            tx = startTx(ignite, txMode);

            final IgniteCache<Integer, Integer> cache00 = cache;

            GridTestUtils.assertThrows(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    cache00.invoke(key, new ExceptionProcessor(63));

                    return null;
                }
            }, EntryProcessorException.class, "Test processor exception.");

            if (tx != null)
                tx.commit();

            checkValue(key, 63);
            assertEquals(8, cntr.get());

            IgniteCache<Integer, Integer> asyncCache = cache.withAsync();

            assertTrue(asyncCache.isAsync());

            assertNull(asyncCache.invoke(key, incProcessor));

            IgniteFuture<Integer> fut = asyncCache.future();

            assertNotNull(fut);

            assertEquals(63, (int)fut.get());
            assertEquals(9, cntr.get());

            checkValue(key, 64);

            tx = startTx(ignite, txMode);

            assertNull(cache.invoke(key, new RemoveProcessor(64)));

            if (tx != null)
                tx.commit();

            checkValue(key, null);
            assertEquals(10, cntr.get());

            assertFalse("Entry processor was invoked from backup", fail);
        }
    }

    /**
     * @param cache0 Cache.
     * @param txMode Not null transaction concurrency mode if explicit transaction should be started.
     * @param client Operations will be performed from client node.
     * @throws Exception If failed.
     */
    private void invokeChain(IgniteCache<Integer, Integer> cache0, @Nullable TransactionConcurrency txMode,
        boolean client, boolean primary) throws Exception {
        IncrementProcessor incProcessor = new IncrementProcessor();

        Ignite ignite = client ? grid(getClientIndex()) :
            (primary ? cache0.unwrap(Ignite.class) : grid(1));

        IgniteCache<Integer, Integer> cache = ignite.cache(cache0.getName());

        cache = cache.withSendValueToBackup();

        for (final Integer key : keys(cache0)) {
            cntr.set(0);

            log.info("Test invoke [key=" + key + ", txMode=" + txMode + ']');

            boolean primary0 = ignite.affinity(null).isPrimary(ignite.cluster().localNode(), key);

            assert (primary && primary0) || (!primary && !primary0);

            cache.remove(key);

            Transaction tx = startTx(ignite, txMode);

            Integer res = cache.invoke(key, incProcessor);
            Integer res1 = cache.invoke(key, incProcessor);
            Integer res2 = cache.invoke(key, incProcessor);

            if (tx != null)
                tx.commit();

            assertEquals(-1, (int)res);
            assertEquals(1, (int)res1);
            assertEquals(2, (int)res2);

            assertEquals(3, cntr.get());

            checkValue(key, 3);

            assertFalse("Entry processor was invoked from backup.", fail);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeAllTwoBackup() throws Exception {
        doTestInvokeAll(new CacheConfiguration<Integer, Integer>()
            .setBackups(2)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setAtomicityMode(TRANSACTIONAL)
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeAllTwoBackupPrimarySync() throws Exception {
        doTestInvokeAll(new CacheConfiguration<Integer, Integer>()
            .setBackups(2)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC)
            .setAtomicityMode(TRANSACTIONAL)
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void doTestInvokeAll(CacheConfiguration<Integer, Integer> ccfg) throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).createCache(ccfg);

        try {
            invokeAll(cache, null);

            invokeAll(cache, PESSIMISTIC);

            invokeAll(cache, OPTIMISTIC);
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
     * @param cache Cache.
     * @param txMode Not null transaction concurrency mode if explicit transaction should be started.
     * @throws Exception If failed.
     */
    private void invokeAll(IgniteCache<Integer, Integer> cache, @Nullable TransactionConcurrency txMode)
        throws Exception {
        invokeAll(cache, new HashSet<>(primaryKeys(cache, 3, 0)), txMode);

        invokeAll(cache, new HashSet<>(backupKeys(cache, 3, 0)), txMode);

        Set<Integer> keys = new HashSet<>();

        for (int i = 0; i < getServerNodeCount(); i++)
            keys.addAll(primaryKeys(jcache(i), 3, 0));

        invokeAll(cache, keys, txMode);

        keys = new HashSet<>();

        for (int i = 0; i < 1000; i++)
            keys.add(i);

        invokeAll(cache, keys, txMode);
    }

    /**
     * @param cache Cache.
     * @param keys Keys.
     * @param txMode Not null transaction concurrency mode if explicit transaction should be started.
     * @throws Exception If failed.
     */
    private void invokeAll(IgniteCache<Integer, Integer> cache, Set<Integer> keys,
        @Nullable TransactionConcurrency txMode) throws Exception {
        cache.removeAll(keys);

        cache = cache.withSendValueToBackup();

        log.info("Test invokeAll [keys=" + keys + ", txMode=" + txMode + ']');

        IncrementProcessor incProcessor = new IncrementProcessor();

        {
            Transaction tx = startTx(cache.unwrap(Ignite.class), txMode);

            Map<Integer, EntryProcessorResult<Integer>> resMap = cache.invokeAll(keys, incProcessor);

            if (tx != null)
                tx.commit();

            Map<Object, Object> exp = new HashMap<>();

            for (Integer key : keys)
                exp.put(key, -1);

            checkResult(resMap, exp);

            for (Integer key : keys)
                checkValue(key, 1);
        }

        {
            Transaction tx = startTx(cache.unwrap(Ignite.class), txMode);

            Map<Integer, EntryProcessorResult<TestValue>> resMap = cache.invokeAll(keys, new UserClassValueProcessor());

            if (tx != null)
                tx.commit();

            Map<Object, Object> exp = new HashMap<>();

            for (Integer key : keys)
                exp.put(key, new TestValue("1"));

            checkResult(resMap, exp);

            for (Integer key : keys)
                checkValue(key, 1);
        }

        {
            Transaction tx = startTx(cache.unwrap(Ignite.class), txMode);

            Map<Integer, EntryProcessorResult<Collection<TestValue>>> resMap =
                cache.invokeAll(keys, new CollectionReturnProcessor());

            if (tx != null)
                tx.commit();

            Map<Object, Object> exp = new HashMap<>();

            for (Integer key : keys) {
                List<TestValue> expCol = new ArrayList<>();

                for (int i = 0; i < 10; i++)
                    expCol.add(new TestValue("2"));

                exp.put(key, expCol);
            }

            checkResult(resMap, exp);

            for (Integer key : keys)
                checkValue(key, 1);
        }

        {
            Transaction tx = startTx(cache.unwrap(Ignite.class), txMode);

            Map<Integer, EntryProcessorResult<Integer>> resMap = cache.invokeAll(keys, incProcessor);

            if (tx != null)
                tx.commit();

            Map<Object, Object> exp = new HashMap<>();

            for (Integer key : keys)
                exp.put(key, 1);

            checkResult(resMap, exp);

            for (Integer key : keys)
                checkValue(key, 2);
        }

        {
            Transaction tx = startTx(cache.unwrap(Ignite.class), txMode);

            Map<Integer, EntryProcessorResult<Integer>> resMap =
                cache.invokeAll(keys, new ArgumentsSumProcessor(), 10, 20, 30);

            if (tx != null)
                tx.commit();

            Map<Object, Object> exp = new HashMap<>();

            for (Integer key : keys)
                exp.put(key, 3);

            checkResult(resMap, exp);

            for (Integer key : keys)
                checkValue(key, 62);
        }

        {
            Transaction tx = startTx(cache.unwrap(Ignite.class), txMode);

            Map<Integer, EntryProcessorResult<Integer>> resMap = cache.invokeAll(keys, new ExceptionProcessor(null));

            if (tx != null)
                tx.commit();

            for (Integer key : keys) {
                final EntryProcessorResult<Integer> res = resMap.get(key);

                assertNotNull("No result for " + key);

                GridTestUtils.assertThrows(log, new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        res.get();

                        return null;
                    }
                }, EntryProcessorException.class, "Test processor exception.");
            }

            for (Integer key : keys)
                checkValue(key, 62);
        }

        {
            Transaction tx = startTx(cache.unwrap(Ignite.class), txMode);

            Map<Integer, EntryProcessor<Integer, Integer, Integer>> invokeMap = new HashMap<>();

            for (Integer key : keys) {
                switch (key % 4) {
                    case 0: invokeMap.put(key, new IncrementProcessor()); break;

                    case 1: invokeMap.put(key, new RemoveProcessor(62)); break;

                    case 2: invokeMap.put(key, new ArgumentsSumProcessor()); break;

                    case 3: invokeMap.put(key, new ExceptionProcessor(62)); break;

                    default:
                        fail();
                }
            }

            Map<Integer, EntryProcessorResult<Integer>> resMap = cache.invokeAll(invokeMap, 10, 20, 30);

            if (tx != null)
                tx.commit();

            for (Integer key : keys) {
                final EntryProcessorResult<Integer> res = resMap.get(key);

                switch (key % 4) {
                    case 0: {
                        assertNotNull("No result for " + key, res);

                        assertEquals(62, (int)res.get());

                        checkValue(key, 63);

                        break;
                    }

                    case 1: {
                        assertNull(res);

                        checkValue(key, null);

                        break;
                    }

                    case 2: {
                        assertNotNull("No result for " + key, res);

                        assertEquals(3, (int)res.get());

                        checkValue(key, 122);

                        break;
                    }

                    case 3: {
                        assertNotNull("No result for " + key, res);

                        GridTestUtils.assertThrows(log, new Callable<Void>() {
                            @Override public Void call() throws Exception {
                                res.get();

                                return null;
                            }
                        }, EntryProcessorException.class, "Test processor exception.");

                        checkValue(key, 62);

                        break;
                    }
                }
            }
        }

        cache.invokeAll(keys, new IncrementProcessor());

        {
            Transaction tx = startTx(cache.unwrap(Ignite.class), txMode);

            Map<Integer, EntryProcessorResult<Integer>> resMap = cache.invokeAll(keys, new RemoveProcessor(null));

            if (tx != null)
                tx.commit();

            assertEquals("Unexpected results: " + resMap, 0, resMap.size());

            for (Integer key : keys)
                checkValue(key, null);
        }

        IgniteCache<Integer, Integer> asyncCache = cache.withAsync();

        assertTrue(asyncCache.isAsync());

        assertNull(asyncCache.invokeAll(keys, new IncrementProcessor()));

        IgniteFuture<Map<Integer, EntryProcessorResult<Integer>>> fut = asyncCache.future();

        Map<Integer, EntryProcessorResult<Integer>> resMap = fut.get();

        Map<Object, Object> exp = new HashMap<>();

        for (Integer key : keys)
            exp.put(key, -1);

        checkResult(resMap, exp);

        for (Integer key : keys)
            checkValue(key, 1);

        Map<Integer, EntryProcessor<Integer, Integer, Integer>> invokeMap = new HashMap<>();

        for (Integer key : keys)
            invokeMap.put(key, incProcessor);

        assertNull(asyncCache.invokeAll(invokeMap));

        fut = asyncCache.future();

        resMap = fut.get();

        for (Integer key : keys)
            exp.put(key, 1);

        checkResult(resMap, exp);

        for (Integer key : keys)
            checkValue(key, 2);

        assertFalse("Entry processor was invoked from backup", fail);
    }

    /**
     * @param resMap Result map.
     * @param exp Expected results.
     */
    @SuppressWarnings("unchecked")
    private void checkResult(Map resMap, Map<Object, Object> exp) {
        assertNotNull(resMap);

        assertEquals(exp.size(), resMap.size());

        for (Map.Entry<Object, Object> expVal : exp.entrySet()) {
            EntryProcessorResult<?> res = (EntryProcessorResult)resMap.get(expVal.getKey());

            assertNotNull("No result for " + expVal.getKey(), res);

            assertEquals("Unexpected result for " + expVal.getKey(), res.get(), expVal.getValue());
        }
    }

    /**
     * @param key Key.
     * @param expVal Expected value.
     */
    protected void checkValue(final Object key, @Nullable final Object expVal) throws Exception {
        if (expVal != null) {
            for (int i = 0; i < getServerNodeCount(); i++) {
                final IgniteCache<Object, Object> cache = jcache(i);

                GridTestUtils.waitForCondition(new PA() {
                    @Override public boolean apply() {
                        Object o = cache.localPeek(key, CachePeekMode.ONHEAP);

                        return o != null && o.equals(expVal);
                    }
                }, 100L);

                Object val = cache.localPeek(key, CachePeekMode.ONHEAP);

                if (val == null)
                    assertFalse(ignite(0).affinity(null).isPrimaryOrBackup(ignite(i).cluster().localNode(), key));
                else
                    assertEquals("Unexpected value for grid " + i, expVal, val);
            }
        }
        else {
            for (int i = 0; i < getServerNodeCount(); i++) {
                final IgniteCache<Object, Object> cache = jcache(i);

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
     * @return Test keys.
     * @throws Exception If failed.
     */
    protected Collection<Integer> keys(IgniteCache<Integer, Integer> cache) throws Exception {
        ArrayList<Integer> keys = new ArrayList<>();

        keys.add(primaryKeys(cache, 1, lastKey).get(0));

        lastKey = Collections.max(keys) + 1;

        return keys;
    }

    /**
     * @param txMode Transaction concurrency mode.
     * @return Transaction.
     */
    @Nullable private Transaction startTx(Ignite ignite, @Nullable TransactionConcurrency txMode) {
        return txMode == null ? null : ignite.transactions().txStart(txMode, REPEATABLE_READ);
    }

    /**
     *
     */
    private static class ArgumentsSumProcessor extends AbstractEntryProcessor<Integer, Integer, Integer> {
        /** {@inheritDoc} */
        @Override public Integer process(MutableEntry<Integer, Integer> e, Object... args)
            throws EntryProcessorException {
            super.process(e, args);

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
        @Override public String process(MutableEntry<Integer, Integer> e, Object... arguments)
            throws EntryProcessorException {
            super.process(e, arguments);

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
        @Override public TestValue process(MutableEntry<Integer, Integer> e, Object... arguments)
            throws EntryProcessorException {
            super.process(e, arguments);

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
        @Override public Collection<TestValue> process(MutableEntry<Integer, Integer> e, Object... arguments)
            throws EntryProcessorException {
            super.process(e, arguments);

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
        @Override public Integer process(MutableEntry<Integer, Integer> e, Object... arguments)
            throws EntryProcessorException {
            super.process(e, arguments);
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
            Object... arguments) throws EntryProcessorException {
            super.process(e, arguments);

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
    private static class ExceptionProcessor extends AbstractEntryProcessor<Integer, Integer, Integer> {
        /** */
        private Integer expVal;

        /**
         * @param expVal Expected value.
         */
        ExceptionProcessor(@Nullable Integer expVal) {
            this.expVal = expVal;
        }

        /** {@inheritDoc} */
        @Override public Integer process(MutableEntry<Integer, Integer> e,
            Object... arguments) throws EntryProcessorException {
            super.process(e, arguments);

            assertTrue(e.exists());

            if (expVal != null)
                assertEquals(expVal, e.getValue());

            throw new EntryProcessorException("Test processor exception.");
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ExceptionProcessor.class, this);
        }
    }

    /**
     *
     */
    private static class AbstractEntryProcessor<K, V, T> implements EntryProcessor<K, V, T> {
        /** {@inheritDoc} */
        @Override public T process(MutableEntry<K, V> entry, Object... arguments) throws EntryProcessorException {
            Ignite ignite = entry.unwrap(Ignite.class);

            cntr.incrementAndGet();

            if (!ExceptionUtils.getStackTrace(new Exception("Check stack trace")).contains("addInvokeResult") &&
                !ignite.affinity(null).isPrimary(ignite.cluster().localNode(), entry.getKey())) {
                ignite.log().error("Failed. Entry processor invoked on backup node.");

                U.dumpStack("Node id: " + ignite.cluster().localNode().id() + ", key: " + entry.getKey()
                    + ", val: " + entry.getValue());

                fail = true;
            }

            return null;
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