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

package org.apache.ignite.internal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

/**
 * Test exchange manager warnings.
 */
public class GridCacheHashMapPutAllWarningsTest extends GridCommonAbstractTest {
    /** */
    private ListeningTestLogger testLog;

    /** */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.setGridLogger(testLog);

        return cfg;
    }

    /** */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testHashMapPutAllExactMessage() throws Exception {
        List<String> messages = Collections.synchronizedList(new ArrayList<>());

        testLog = new ListeningTestLogger(false, log());

        testLog.registerListener((s) -> {
                if (s.contains("deadlock"))
                    messages.add(s);
            });

        Ignite ignite = startGrid(0);

        IgniteCache<Integer, String> c = ignite.getOrCreateCache(new CacheConfiguration<>("exact"));

        HashMap<Integer, String> m = new HashMap<>();

        m.put(1, "foo");
        m.put(2, "bar");

        c.putAll(m);

        assertEquals(2, c.size());

        int found = 0;

        for (String message : messages) {
            if (message.contains("Unordered map java.util.HashMap is used for putAll operation on cache exact. " +
                "This can lead to a distributed deadlock. Switch to a sorted map like TreeMap instead."))
                found++;
        }

        assertEquals(1, found);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testHashMapPutAllExplicitOptimistic() throws Exception {
        if (MvccFeatureChecker.forcedMvcc())
            return;

        List<String> messages = Collections.synchronizedList(new ArrayList<>());

        testLog = new ListeningTestLogger(false, log());

        testLog.registerListener((s) -> {
            if (s.contains("deadlock"))
                messages.add(s);
        });

        Ignite ignite = startGrid(0);

        IgniteCache<Integer, String> c = ignite.getOrCreateCache(new CacheConfiguration<Integer, String>("explicitTx")
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        ignite.transactions().txStart(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.SERIALIZABLE);

        HashMap<Integer, String> m = new HashMap<>();

        m.put(1, "foo");
        m.put(2, "bar");

        c.putAllAsync(m);

        ignite.transactions().tx().commit();

        assertEquals(2, c.size());

        for (String message : messages) {
            assertFalse(message.contains("Unordered map"));
            assertFalse(message.contains("operation on cache"));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testHashMapInvokeAllLocal() throws Exception {
        List<String> messages = Collections.synchronizedList(new ArrayList<>());

        testLog = new ListeningTestLogger(false, log());

        testLog.registerListener((s) -> {
            if (s.contains("deadlock"))
                messages.add(s);
        });

        Ignite ignite = startGrid(0);

        IgniteCache<Integer, String> c = ignite.getOrCreateCache(new CacheConfiguration<Integer, String>("invoke")
            .setCacheMode(CacheMode.LOCAL));

        c.put(1, "foo");
        c.put(2, "bar");

        Map<Integer, EntryProcessorResult<String>> result = c.invokeAll(new HashSet<>(Arrays.asList(1, 2)),
            new EntryProcessor<Integer, String, String>() {
                @Override public String process(MutableEntry entry, Object... arguments) throws EntryProcessorException {
                    String newVal = entry.getValue() + "2";

                    entry.setValue(newVal);

                    return newVal;
                }
            });

        assertEquals(2, result.size());
        assertEquals("bar2", c.get(2));

        int found = 0;

        for (String message : messages) {
            if (message.contains("Unordered collection java.util.HashSet is used for invokeAll operation on cache invoke. "))
                found++;
        }

        assertEquals(1, found);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTreeMapRemoveAll() throws Exception {
        List<String> messages = Collections.synchronizedList(new ArrayList<>());

        testLog = new ListeningTestLogger(false, log());

        testLog.registerListener((s) -> {
            if (s.contains("deadlock"))
                messages.add(s);
        });

        Ignite ignite = startGrid(0);

        IgniteCache<Integer, String> c = ignite.getOrCreateCache(new CacheConfiguration<Integer, String>("remove")
            .setCacheMode(CacheMode.PARTITIONED));

        c.put(1, "foo");
        c.put(2, "bar");

        c.removeAll(new TreeSet<>(Arrays.asList(1, 3)));

        assertEquals(1, c.size());

        int found = 0;

        for (String message : messages) {
            if (message.contains("Unordered collection "))
                found++;

            if (message.contains("operation on cache"))
                found++;
        }

        assertEquals(0, found);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTreeMapRemoveAllEntries() throws Exception {
        List<String> messages = Collections.synchronizedList(new ArrayList<>());

        testLog = new ListeningTestLogger(false, log());

        testLog.registerListener((s) -> {
            if (s.contains("deadlock"))
                messages.add(s);
        });

        Ignite ignite = startGrid(0);
        startGrid(1);

        IgniteCache<Integer, String> c = ignite.getOrCreateCache(new CacheConfiguration<Integer, String>("entries")
            .setCacheMode(CacheMode.REPLICATED)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setBackups(1));

        for (int i = 0; i < 1000; i++) {
            c.put(i, "foo");
            c.put(i * 2, "bar");
        }

        c.removeAll();

        assertEquals(0, c.size());

        for (String message : messages) {
            assertFalse(message.contains("Unordered collection "));

            assertFalse(message.contains("operation on cache"));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTreeMapClearEntries() throws Exception {
        List<String> messages = Collections.synchronizedList(new ArrayList<>());

        testLog = new ListeningTestLogger(false, log());

        testLog.registerListener((s) -> {
            if (s.contains("deadlock"))
                messages.add(s);
        });

        Ignite ignite = startGrid(0);
        startGrid(1);

        IgniteCache<Integer, String> c = ignite.getOrCreateCache(new CacheConfiguration<Integer, String>("entries")
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setBackups(1));

        for (int i = 0; i < 1000; i++) {
            c.put(i, "foo");
            c.put(i * 2, "bar");
        }

        c.clear();

        assertEquals(0, c.size());

        for (String message : messages) {
            assertFalse(message.contains("Unordered "));

            assertFalse(message.contains("operation on cache"));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testHashSetGetAllReplicated() throws Exception {
        List<String> messages = Collections.synchronizedList(new ArrayList<>());

        testLog = new ListeningTestLogger(false, log());

        testLog.registerListener((s) -> {
            if (s.contains("deadlock"))
                messages.add(s);
        });

        Ignite ignite = startGrid(0);

        IgniteCache<Integer, String> c = ignite.getOrCreateCache(new CacheConfiguration<Integer, String>("get")
            .setCacheMode(CacheMode.REPLICATED));

        c.put(1, "foo");
        c.put(2, "bar");

        assertEquals(1, c.getAll(new HashSet<>(Arrays.asList(1, 3))).size());

        int found = 0;

        for (String message : messages) {
            if (message.contains("Unordered collection "))
                found++;

            if (message.contains("operation on cache"))
                found++;
        }

        assertEquals(0, found);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testHashSetGetAllTx() throws Exception {
        List<String> messages = Collections.synchronizedList(new ArrayList<>());

        testLog = new ListeningTestLogger(false, log());

        testLog.registerListener((s) -> {
            if (s.contains("deadlock"))
                messages.add(s);
        });

        Ignite ignite = startGrid(0);

        IgniteCache<Integer, String> c = ignite.getOrCreateCache(new CacheConfiguration<Integer, String>("getTx")
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setCacheMode(CacheMode.PARTITIONED));

        c.put(1, "foo");
        c.put(2, "bar");

        try (Transaction tx = ignite.transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
            assertEquals(1, c.getAll(new HashSet<>(Arrays.asList(1, 3))).size());

            tx.commit();
        }

        int found = 0;

        for (String message : messages) {
            if (message.contains("Unordered collection java.util.HashSet is used for getAll operation on cache getTx."))
                found++;
        }

        assertEquals(1, found);
    }
}
