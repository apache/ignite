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

package org.apache.ignite.client;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/** */
public class ClientOrderedCollectionWarnTest extends GridCommonAbstractTest {
    /** */
    private static final String WARN_LSNR_MSG = "Unordered %s java.util.%s is used for"; //

    /** */
    private static final LogListener LINKED_HASH_MAP_WARN_LSNR =
        LogListener.matches(String.format(WARN_LSNR_MSG, "map", "LinkedHashMap")).times(1).build();

    /** */
    private static final LogListener HASH_MAP_WARN_LSNR =
        LogListener.matches(String.format(WARN_LSNR_MSG, "map", "HashMap")).times(1).build();

    /** */
    private static final LogListener LINKED_HASH_SET_WARN_LSNR =
        LogListener.matches(String.format(WARN_LSNR_MSG, "collection", "LinkedHashSet")).times(1).build();

    /** */
    private static final LogListener HASH_SET_WARN_LSNR =
        LogListener.matches(String.format(WARN_LSNR_MSG, "collection", "HashSet")).times(1).build();

    /** */
    private static final List<LogListener> MAP_LSNR = List.of(LINKED_HASH_MAP_WARN_LSNR, HASH_MAP_WARN_LSNR);

    /** */
    private static final List<LogListener> SET_LSNR = List.of(LINKED_HASH_SET_WARN_LSNR, HASH_SET_WARN_LSNR);

    /** */
    private final ListeningTestLogger testLog = new ListeningTestLogger(log);

    /** */
    private static IgniteEx ign;

    /** */
    private static IgniteClient cli;

    /** */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        testLog.registerListener(LINKED_HASH_MAP_WARN_LSNR);
        testLog.registerListener(HASH_MAP_WARN_LSNR);
        testLog.registerListener(LINKED_HASH_SET_WARN_LSNR);
        testLog.registerListener(HASH_SET_WARN_LSNR);

        cfg.setGridLogger(testLog);

        return cfg;
    }

    /** */
    private ClientConfiguration getClientConfiguration() {
        return new ClientConfiguration().setAddresses(Config.SERVER).setLogger(testLog);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        ign = startGrid();
        cli = Ignition.startClient(getClientConfiguration());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        ign.close();
        cli.close();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        LINKED_HASH_MAP_WARN_LSNR.reset();
        HASH_MAP_WARN_LSNR.reset();
        LINKED_HASH_SET_WARN_LSNR.reset();
        HASH_SET_WARN_LSNR.reset();
    }

    /** */
    @Test
    public void testPutAll() throws Exception {
        testPutAll(new TreeMap<>(), MAP_LSNR, false);
    }

    /** */
    @Test
    public void testPutAllWarn() throws Exception {
        testPutAll(new HashMap<>(), List.of(HASH_MAP_WARN_LSNR), true);
    }

    /** */
    @Test
    public void testInvokeAll() throws Exception {
        testInvokeAll(new TreeSet<>(), SET_LSNR, false);
    }

    /** */
    @Test
    public void testInvokeAllWarn() throws Exception {
        testInvokeAll(new HashSet<>(), List.of(HASH_SET_WARN_LSNR), true);
    }

    /** */
    @Test
    public void testRemoveAll() throws Exception {
        testSetAllOp(new TreeSet<>(), ClientCache::removeAll);

        testSetAllOp(new HashSet<>(), ClientCache::removeAll);
    }

    /** */
    @Test
    public void testGetAll() throws Exception {
        testSetAllOp(new TreeSet<>(), ClientCache::getAll);

        testSetAllOp(new HashSet<>(), ClientCache::getAll);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutAllExplicitOptimistic() throws Exception {
        testPutAllWithTx(new HashMap<>(), OPTIMISTIC, MAP_LSNR, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutAllExplicitPessimistic() throws Exception {
        testPutAllWithTx(new HashMap<>(), PESSIMISTIC, List.of(HASH_MAP_WARN_LSNR), true);
    }

    /** */
    private void testPutAll(
        Map<Long, Long> map,
        List<LogListener> lsnrs,
        boolean warnPresent
    ) throws Exception {
        createTransactionalCache().putAll(fillMap(map));

        checkOp(warnPresent, lsnrs);
    }

    /** */
    private void testInvokeAll(
        Set<Long> set,
        List<LogListener> lsnrs,
        boolean warnPresent
    ) throws Exception {
        createTransactionalCache().invokeAll(fillSet(set), new TestEntryProcessor());

        checkOp(warnPresent, lsnrs);
    }

    /** */
    private void testSetAllOp(
        Set<Long> set,
        BiConsumer<ClientCache, Set> cacheOp
    ) throws Exception {
        cacheOp.accept(createTransactionalCache(), fillSet(set));

        checkOp(false, SET_LSNR);
    }

    /** */
    private void testPutAllWithTx(
        Map<Long, Long> map,
        TransactionConcurrency concurrency,
        List<LogListener> lsnrs,
        boolean warnPresent
    ) throws Exception {
        ClientCache<Long, Long> c = createTransactionalCache();

        ClientTransaction tx = cli.transactions().txStart(concurrency, SERIALIZABLE);

        c.putAll(fillMap(map));

        tx.commit();
        tx.close();

        checkOp(warnPresent, lsnrs);
    }

    /** */
    private void checkOp(boolean warnPresent, List<LogListener> lsnrs) throws Exception {
        for (LogListener lsnr : lsnrs) {
            if (warnPresent)
                assertTrue(waitForCondition(lsnr::check, getTestTimeout()));
            else
                assertFalse(lsnr.check());
        }
    }

    /** */
    private <K, V> ClientCache<K, V> createTransactionalCache() {
        ClientCacheConfiguration cacheCfg = new ClientCacheConfiguration()
            .setName(DEFAULT_CACHE_NAME)
            .setAtomicityMode(TRANSACTIONAL);

        return cli.getOrCreateCache(cacheCfg);
    }

    /** */
    private Map<Long, Long> fillMap(Map<Long, Long> map) {
        map.put(0L, 0L);
        map.put(1L, 1L);

        return map;
    }

    /** */
    private Set<Long> fillSet(Set<Long> set) {
        set.add(0L);
        set.add(1L);

        return set;
    }

    /** */
    private static class TestEntryProcessor implements EntryProcessor<Object, Object, Boolean> {
        /** {@inheritDoc} */
        @Override public Boolean process(MutableEntry<Object, Object> entry, Object... args) {
            return true;
        }
    }
}
