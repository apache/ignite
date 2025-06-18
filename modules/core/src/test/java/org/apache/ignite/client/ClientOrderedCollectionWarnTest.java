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
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
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
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/** */
public class ClientOrderedCollectionWarnTest extends GridCommonAbstractTest {
    /** */
    private static final String WARN_LSNR_MSG = "Unordered %s java.util."; //

    /** */
    private static final LogListener MAP_WARN_LSNR =
        LogListener.matches(String.format(WARN_LSNR_MSG, "map")).times(1).build();

    /** */
    private static final LogListener SET_WARN_LSNR =
        LogListener.matches(String.format(WARN_LSNR_MSG, "collection")).times(1).build();

    /** */
    private final ListeningTestLogger testLog = new ListeningTestLogger(log);

    /** */
    private static IgniteEx ign;

    /** */
    private static IgniteClient cli;

    /** */
    private static ClientCache<Long, Long> cache;

    /** */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        testLog.registerListener(MAP_WARN_LSNR);
        testLog.registerListener(SET_WARN_LSNR);

        cfg.setGridLogger(testLog);

        return cfg;
    }

    /** */
    private ClientConfiguration getClientConfiguration() {
        return new ClientConfiguration().setAddresses(Config.SERVER).setLogger(testLog);
    }

    /** */
    private ClientCacheConfiguration getClientClientConfiguration() {
        return new ClientCacheConfiguration()
            .setName(DEFAULT_CACHE_NAME)
            .setAtomicityMode(TRANSACTIONAL);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        ign = startGrid();
        cli = Ignition.startClient(getClientConfiguration());

        cache = cli.getOrCreateCache(getClientClientConfiguration());
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

        MAP_WARN_LSNR.reset();
        SET_WARN_LSNR.reset();
    }

    /** */
    @Test
    public void putAll() throws Exception {
        Runnable cacheOpTreeMap = () -> cache.putAll(fillMap(new TreeMap<>()));
        Runnable cacheOpHashMap = () -> cache.putAll(fillMap(new HashMap<>()));

        testOp(cacheOpTreeMap, cacheOpHashMap, MAP_WARN_LSNR);
    }

    /** */
    @Test
    public void invokeAll() throws Exception {
        Runnable cacheOpTreeSet = () -> cache.invokeAll(fillSet(new TreeSet<>()), new TestEntryProcessor());
        Runnable cacheOpHashSet = () -> cache.invokeAll(fillSet(new HashSet<>()), new TestEntryProcessor());

        testOp(cacheOpTreeSet, cacheOpHashSet, SET_WARN_LSNR);
    }

    /** */
    @Test
    public void testRemoveAll() throws Exception {
        Runnable cacheOpTreeSet = () -> cache.removeAll(fillSet(new TreeSet<>()));
        Runnable cacheOpHashSet = () -> cache.removeAll(fillSet(new HashSet<>()));

        testOp(cacheOpTreeSet, cacheOpHashSet, SET_WARN_LSNR);
    }

    /** */
    @Test
    public void testGetAll() throws Exception {
        Runnable cacheOpTreeSet = () -> cache.removeAll(fillSet(new TreeSet<>()));
        Runnable cacheOpHashSet = () -> cache.removeAll(fillSet(new HashSet<>()));

        testOp(cacheOpTreeSet, cacheOpHashSet, SET_WARN_LSNR);

        SET_WARN_LSNR.reset();

        withTx(cacheOpHashSet, PESSIMISTIC, READ_COMMITTED);

        checkOp(false, SET_WARN_LSNR);
    }

    /** */
    private void testOp(Runnable cacheOpWithOrdered, Runnable cacheOpWithNoOrdered, LogListener lsnr) throws Exception {
        cacheOpWithOrdered.run();

        withTx(cacheOpWithOrdered, PESSIMISTIC, SERIALIZABLE);

        withTx(cacheOpWithNoOrdered, OPTIMISTIC, SERIALIZABLE);

        checkOp(false, lsnr);

        withTx(cacheOpWithNoOrdered, PESSIMISTIC, READ_COMMITTED);

        checkOp(true, lsnr);
    }

    /** */
    private void withTx(Runnable cacheOp, TransactionConcurrency concurrency, TransactionIsolation isolation) {
        try (ClientTransaction tx = cli.transactions().txStart(concurrency, isolation)) {
            cacheOp.run();

            tx.commit();
        }
    }

    /** */
    private void checkOp(boolean warnPresent, LogListener lsnr) throws Exception {
        if (warnPresent)
            assertTrue(waitForCondition(lsnr::check, getTestTimeout()));
        else
            assertFalse(lsnr.check());
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
    private static class TestEntryProcessor implements EntryProcessor<Long, Long, Boolean> {
        /** {@inheritDoc} */
        @Override public Boolean process(MutableEntry<Long, Long> entry, Object... args) {
            return true;
        }
    }
}
