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

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.junits.IgniteCacheConfigVariationsAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.testframework.junits.IgniteConfigVariationsAbstractTest.DataMode.PLANE_OBJECT;
import static org.apache.ignite.testframework.junits.IgniteConfigVariationsAbstractTest.DataMode.SERIALIZABLE;

/**
 *
 */
@SuppressWarnings("unchecked")
public class WithKeepBinaryCacheFullApiTest extends IgniteCacheConfigVariationsAbstractTest {
    /** */
    protected static volatile boolean interceptorBinaryObjExp = true;

    /** */
    public static final int CNT = 10;

    /** */
    public static final CacheEntryProcessor NOOP_ENTRY_PROC = new CacheEntryProcessor() {
        @Override public Object process(MutableEntry entry, Object... arguments) throws EntryProcessorException {
            return null;
        }
    };

    /** */
    public static final CacheEntryProcessor INC_ENTRY_PROC_USER_OBJ = new CacheEntryProcessor() {
        @Override public Object process(MutableEntry entry, Object... arguments) throws EntryProcessorException {
            assertTrue(entry.getKey() instanceof BinaryObject);

            Object val = entry.getValue();

            int valId = 0;

            if (val != null) {
                assertTrue(val instanceof BinaryObject);

                valId = valueOf(((BinaryObject)val).deserialize()) + 1;
            }

            Object newVal = value(valId, (DataMode)arguments[0]);

            assertFalse(newVal instanceof BinaryObject);

            entry.setValue(newVal);

            return val == null ? null : ((BinaryObject)val).deserialize();
        }
    };

    /** */
    public static final CacheEntryProcessor INC_ENTRY_PROC_BINARY_OBJ = new CacheEntryProcessor() {
        @Override public Object process(MutableEntry entry, Object... arguments) throws EntryProcessorException {
            assertTrue(entry.getKey() instanceof BinaryObject);

            Object val = entry.getValue();

            int valId = 0;

            if (val != null) {
                assertTrue(val instanceof BinaryObject);

                valId = valueOf(((BinaryObject)val).deserialize()) + 1;
            }

            Object newVal = value(valId, (DataMode)arguments[0]);

            Object newBinaryVal = ((Ignite)entry.unwrap(Ignite.class)).binary().toBinary(newVal);

            entry.setValue(newBinaryVal);

            return val;
        }
    };

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration() {
        CacheConfiguration cc = super.cacheConfiguration();

        cc.setStoreKeepBinary(true);

        return cc;
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("serial")
    @Test
    public void testRemovePutGet() throws Exception {
        runInAllDataModes(new TestRunnable() {
            @Override public void run() throws Exception {
                final IgniteCache cache = jcache().withKeepBinary();

                final Set keys = new LinkedHashSet() {{
                    for (int i = 0; i < CNT; i++)
                        add(key(i));
                }};

                runInAllTxModes(new TestRunnable() {
                    @Override public void run() throws Exception {
                        for (Object key : keys)
                            cache.remove(key);
                    }
                });

                runInAllTxModes(new TestRunnable() {
                    @Override public void run() throws Exception {
                        for (Object key : keys) {
                            assertNull(cache.get(key));
                            assertNull(cache.getEntry(key));
                        }
                    }
                });

                for (final Object key : keys) {
                    runInAllTxModes(new TestRunnable() {
                        @Override public void run() throws Exception {
                            Object val = value(valueOf(key));

                            cache.put(key, val);

                            BinaryObject retVal = (BinaryObject)cache.get(key);

                            assertEquals(val, retVal.deserialize());

                            CacheEntry<BinaryObject, BinaryObject> entry = cache.getEntry(key);

                            assertTrue(entry.getKey() instanceof BinaryObject);

                            assertEquals(val, entry.getValue().deserialize());

                            assertTrue(cache.remove(key));
                        }
                    });
                }
            }
        }, PLANE_OBJECT, SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("serial")
    @Test
    public void testRemovePutGetAsync() throws Exception {
        runInAllDataModes(new TestRunnable() {
            @Override public void run() throws Exception {
                final IgniteCache cache = jcache().withKeepBinary();

                final Set keys = new LinkedHashSet() {{
                    for (int i = 0; i < CNT; i++)
                        add(key(i));
                }};

                runInAllTxModes(new TestRunnable() {
                    @Override public void run() throws Exception {
                        for (Object key : keys)
                            cache.removeAsync(key).get();
                    }
                });

                runInAllTxModes(new TestRunnable() {
                    @Override public void run() throws Exception {
                        for (Object key : keys) {
                            assertNull(cache.getAsync(key).get());

                            assertNull(cache.getEntryAsync(key).get());
                        }
                    }
                });

                for (final Object key : keys) {
                    runInAllTxModes(new TestRunnable() {
                        @Override public void run() throws Exception {
                            Object val = value(valueOf(key));

                            cache.putAsync(key, val).get();

                            BinaryObject retVal = (BinaryObject)cache.getAsync(key).get();

                            assertEquals(val, retVal.deserialize());

                            CacheEntry<BinaryObject, BinaryObject> e =
                                (CacheEntry<BinaryObject, BinaryObject>)cache.getEntryAsync(key).get();

                            assertEquals(key, deserializeBinary(e.getKey()));

                            assertEquals(val, e.getValue().deserialize());
                        }
                    });
                }
            }
        }, PLANE_OBJECT, SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("serial")
    @Test
    public void testPutAllGetAll() throws Exception {
        runInAllDataModes(new TestRunnable() {
            @Override public void run() throws Exception {
                final IgniteCache cache = jcache().withKeepBinary();

                final Set keys = new LinkedHashSet() {{
                    for (int i = 0; i < CNT; i++)
                        add(key(i));
                }};

                runInAllTxModes(new TestRunnable() {
                    @Override public void run() throws Exception {
                        for (Object val : cache.getAll(keys).values())
                            assertNull(val);
                    }
                });

                runInAllTxModes(new TestRunnable() {
                    @Override public void run() throws Exception {
                        Collection<CacheEntry> entries = cache.<CacheEntry>getEntries(keys);

                        for (CacheEntry e : entries)
                            assertNull(e.getValue());
                    }
                });

                runInAllTxModes(new TestRunnable() {
                    @Override public void run() throws Exception {
                        Map keyValMap = new LinkedHashMap() {{
                            for (Object key : keys) {
                                Object val = value(valueOf(key));

                                put(key, val);
                            }
                        }};

                        cache.putAll(keyValMap);

                        Set<Map.Entry<BinaryObject, BinaryObject>> set = cache.getAll(keys).entrySet();

                        for (Map.Entry<BinaryObject, BinaryObject> e : set) {
                            Object expVal = value(valueOf(e.getKey().deserialize()));

                            assertEquals(expVal, e.getValue().deserialize());
                        }

                        Collection<CacheEntry<BinaryObject, BinaryObject>> entries = cache.getEntries(keys);

                        for (CacheEntry<BinaryObject, BinaryObject> e : entries) {
                            assertTrue(e.getKey() instanceof BinaryObject);

                            Object expVal = value(valueOf(e.getKey().deserialize()));

                            assertEquals(expVal, e.getValue().deserialize());
                        }

                        cache.removeAll(keys);
                    }
                });
            }
        }, PLANE_OBJECT, SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("serial")
    @Test
    public void testPutAllGetAllAsync() throws Exception {
        runInAllDataModes(new TestRunnable() {
            @Override public void run() throws Exception {
                final IgniteCache cache = jcache().withKeepBinary();

                final Set keys = new LinkedHashSet() {{
                    for (int i = 0; i < CNT; i++)
                        add(key(i));
                }};

                runInAllTxModes(new TestRunnable() {
                    @Override public void run() throws Exception {
                        Map res = (Map)cache.getAllAsync(keys).get();

                        for (Object val : res.values())
                            assertNull(val);
                    }
                });

                runInAllTxModes(new TestRunnable() {
                    @Override public void run() throws Exception {
                        Collection<CacheEntry> entries =
                            (Collection<CacheEntry>)cache.<CacheEntry>getEntriesAsync(keys).get();

                        for (CacheEntry e : entries)
                            assertNull(e.getValue());
                    }
                });

                runInAllTxModes(new TestRunnable() {
                    @Override public void run() throws Exception {
                        Map keyValMap = new LinkedHashMap() {{
                            for (Object key : keys) {
                                Object val = value(valueOf(key));

                                put(key, val);
                            }
                        }};

                        cache.putAllAsync(keyValMap).get();

                        Set<Map.Entry<BinaryObject, BinaryObject>> set =
                            ((Map)cache.getAllAsync(keys).get()).entrySet();

                        for (Map.Entry<BinaryObject, BinaryObject> e : set) {
                            Object expVal = value(valueOf(e.getKey().deserialize()));

                            assertEquals(expVal, e.getValue().deserialize());
                        }

                        Collection<CacheEntry<BinaryObject, BinaryObject>> entries =
                            (Collection<CacheEntry<BinaryObject, BinaryObject>>)cache.getEntriesAsync(keys).get();

                        for (CacheEntry<BinaryObject, BinaryObject> e : entries) {
                            assertTrue(e.getKey() instanceof BinaryObject);

                            Object expVal = value(valueOf(e.getKey().deserialize()));

                            assertEquals(expVal, e.getValue().deserialize());
                        }

                        cache.removeAllAsync(keys).get();

                    }
                });
            }
        }, PLANE_OBJECT, SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("serial")
    @Test
    public void testInvoke() throws Exception {
        runInAllDataModes(new TestRunnable() {
            @Override public void run() throws Exception {
                final IgniteCache cache = jcache().withKeepBinary();

                Set keys = new LinkedHashSet() {{
                    for (int i = 0; i < CNT; i++)
                        add(key(i));
                }};

                for (final Object key : keys) {
                    Object res = cache.invoke(key, NOOP_ENTRY_PROC);

                    assertNull(res);

                    assertNull(cache.get(key));
                }

                for (final Object key : keys) {
                    Object res = cache.invoke(key, INC_ENTRY_PROC_BINARY_OBJ, dataMode);

                    assertNull(res);

                    assertEquals(value(0), deserializeBinary(cache.get(key)));

                    res = cache.invoke(key, INC_ENTRY_PROC_BINARY_OBJ, dataMode);

                    assertEquals(value(0), deserializeBinary(res));

                    assertEquals(value(1), deserializeBinary(cache.get(key)));

                    assertTrue(cache.remove(key));
                }

                // TODO IGNITE-2973: should be always false.
                interceptorBinaryObjExp = atomicityMode() == TRANSACTIONAL;

                try {
                    for (final Object key : keys) {
                        Object res = cache.invoke(key, INC_ENTRY_PROC_USER_OBJ, dataMode);

                        assertNull(res);

                        assertEquals(value(0), deserializeBinary(cache.get(key)));

                        res = cache.invoke(key, INC_ENTRY_PROC_USER_OBJ, dataMode);

                        // TODO IGNITE-2953: uncomment the following assert when the issue will be fixed.
//                               assertEquals(value(0), res);

                        assertEquals(value(1), deserializeBinary(cache.get(key)));

                        assertTrue(cache.remove(key));
                    }
                }
                finally {
                    interceptorBinaryObjExp = true;
                }
            }
        }, PLANE_OBJECT, SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInvokeTx() throws Exception {
        if (!txShouldBeUsed())
            return;

        for (TransactionConcurrency conc : TransactionConcurrency.values()) {
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                info(">>>>> Executing test using explicit txs [concurrency=" + conc + ", isolation=" + isolation + "]");

                checkInvokeTx(conc, isolation);

                jcache().removeAll();
            }
        }
    }

    /**
     * @param conc Concurrency
     * @param isolation Isolation.
     * @throws Exception If failed.
     */
    @SuppressWarnings("serial")
    public void checkInvokeTx(final TransactionConcurrency conc, final TransactionIsolation isolation) throws Exception {
        runInAllDataModes(new TestRunnable() {
            @Override public void run() throws Exception {
                final IgniteCache cache = jcache().withKeepBinary();

                Set keys = new LinkedHashSet() {{
                    for (int i = 0; i < CNT; i++)
                        add(key(i));
                }};

                try (Transaction tx = testedGrid().transactions().txStart(conc, isolation)) {
                    for (final Object key : keys) {
                        Object res = cache.invoke(key, NOOP_ENTRY_PROC);

                        assertNull(res);

                        assertNull(cache.get(key));
                    }

                    tx.commit();
                }

                for (final Object key : keys) {
                    Object res;

                    try (Transaction tx = testedGrid().transactions().txStart(conc, isolation)) {
                        res = cache.invoke(key, INC_ENTRY_PROC_BINARY_OBJ, dataMode);

                        tx.commit();
                    }

                    assertNull(res);

                    assertEquals(value(0), deserializeBinary(cache.get(key)));

                    try (Transaction tx = testedGrid().transactions().txStart(conc, isolation)) {
                        res = cache.invoke(key, INC_ENTRY_PROC_BINARY_OBJ, dataMode);

                        tx.commit();
                    }

                    assertEquals(value(0), deserializeBinary(res));

                    assertEquals(value(1), deserializeBinary(cache.get(key)));

                    try (Transaction tx = testedGrid().transactions().txStart(conc, isolation)) {
                        assertTrue(cache.remove(key));

                        tx.commit();
                    }
                }

                // TODO IGNITE-2973: should be always false.
                interceptorBinaryObjExp = atomicityMode() == TRANSACTIONAL;

                try {
                    for (final Object key : keys) {
                        Object res;

                        try (Transaction tx = testedGrid().transactions().txStart(conc, isolation)) {
                            res = cache.invoke(key, INC_ENTRY_PROC_USER_OBJ, dataMode);

                            tx.commit();
                        }

                        assertNull(res);

                        assertEquals(value(0), deserializeBinary(cache.get(key)));

                        try (Transaction tx = testedGrid().transactions().txStart(conc, isolation)) {
                            res = cache.invoke(key, INC_ENTRY_PROC_USER_OBJ, dataMode);

                            tx.commit();
                        }

                        // TODO IGNITE-2953: uncomment the following assert when the issue will be fixed.
//                               assertEquals(value(0), res);

                        assertEquals(value(1), deserializeBinary(cache.get(key)));

                        try (Transaction tx = testedGrid().transactions().txStart(conc, isolation)) {
                            assertTrue(cache.remove(key));

                            tx.commit();
                        }
                    }
                }
                finally {
                    interceptorBinaryObjExp = true;
                }
            }
        }, PLANE_OBJECT, SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("serial")
    @Test
    public void testInvokeAsync() throws Exception {
        runInAllDataModes(new TestRunnable() {
            @Override public void run() throws Exception {
                final IgniteCache cache = jcache().withKeepBinary();

                Set keys = new LinkedHashSet() {{
                    for (int i = 0; i < CNT; i++)
                        add(key(i));
                }};

                for (final Object key : keys) {
                    Object res = cache.invokeAsync(key, NOOP_ENTRY_PROC).get();

                    assertNull(res);

                    assertNull(cache.getAsync(key).get());
                }

                for (final Object key : keys) {
                    Object res = cache.invokeAsync(key, INC_ENTRY_PROC_BINARY_OBJ, dataMode).get();

                    assertNull(res);

                    assertEquals(value(0), deserializeBinary(cache.getAsync(key).get()));

                    res = cache.invokeAsync(key, INC_ENTRY_PROC_BINARY_OBJ, dataMode).get();

                    assertEquals(value(0), deserializeBinary(res));

                    assertEquals(value(1), deserializeBinary(cache.getAsync(key).get()));

                    assertTrue((Boolean)cache.removeAsync(key).get());
                }

                // TODO IGNITE-2973: should be always false.
                interceptorBinaryObjExp = atomicityMode() == TRANSACTIONAL;

                try {
                    for (final Object key : keys) {
                        Object res = cache.invokeAsync(key, INC_ENTRY_PROC_USER_OBJ, dataMode).get();

                        assertNull(res);

                        assertEquals(value(0), deserializeBinary(cache.getAsync(key).get()));

                        res = cache.invokeAsync(key, INC_ENTRY_PROC_USER_OBJ, dataMode).get();

                        // TODO IGNITE-2953: uncomment the following assert when the issue will be fixed.
//                              assertEquals(value(0), res);

                        assertEquals(value(1), deserializeBinary(cache.getAsync(key).get()));

                        assertTrue((Boolean)cache.removeAsync(key).get());
                    }
                }
                finally {
                    interceptorBinaryObjExp = true;
                }
            }
        }, PLANE_OBJECT, SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInvokeAsyncTx() throws Exception {
        if (!txShouldBeUsed())
            return;

        for (TransactionConcurrency conc : TransactionConcurrency.values()) {
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                checkInvokeAsyncTx(conc, isolation);

                jcache().removeAll();
            }
        }
    }

    /**
     * @param conc Concurrency.
     * @param isolation Isolation.
     * @throws Exception If failed.
     */
    @SuppressWarnings("serial")
    public void checkInvokeAsyncTx(final TransactionConcurrency conc, final TransactionIsolation isolation) throws Exception {
        runInAllDataModes(new TestRunnable() {
            @Override public void run() throws Exception {
                final IgniteCache cache = jcache().withKeepBinary();

                Set keys = new LinkedHashSet() {{
                    for (int i = 0; i < CNT; i++)
                        add(key(i));
                }};

                try (Transaction tx = testedGrid().transactions().txStart(conc, isolation)) {
                    for (final Object key : keys) {
                        Object res = cache.invokeAsync(key, NOOP_ENTRY_PROC).get();

                        assertNull(res);

                        assertNull(cache.getAsync(key).get());
                    }

                    tx.commit();
                }

                for (final Object key : keys) {
                    Object res;

                    try (Transaction tx = testedGrid().transactions().txStart(conc, isolation)) {
                        res = cache.invokeAsync(key, INC_ENTRY_PROC_BINARY_OBJ, dataMode).get();

                        tx.commit();
                    }

                    assertNull(res);

                    assertEquals(value(0), deserializeBinary(cache.getAsync(key).get()));

                    IgniteFuture f;

                    try (Transaction tx = testedGrid().transactions().txStart(conc, isolation)) {
                        f = cache.invokeAsync(key, INC_ENTRY_PROC_BINARY_OBJ, dataMode);

                        tx.commit();
                    }

                    res = f.get();

                    assertEquals(value(0), deserializeBinary(res));

                    assertEquals(value(1), deserializeBinary(cache.getAsync(key).get()));

                    try (Transaction tx = testedGrid().transactions().txStart(conc, isolation)) {
                        assertTrue((Boolean)cache.removeAsync(key).get());

                        tx.commit();
                    }
                }

                // TODO IGNITE-2973: should be always false.
                interceptorBinaryObjExp = atomicityMode() == TRANSACTIONAL;

                try {
                    for (final Object key : keys) {
                        Object res;

                        try (Transaction tx = testedGrid().transactions().txStart(conc, isolation)) {
                            res = cache.invokeAsync(key, INC_ENTRY_PROC_USER_OBJ, dataMode).get();

                            tx.commit();
                        }

                        assertNull(res);

                        assertEquals(value(0), deserializeBinary(cache.getAsync(key).get()));

                        try (Transaction tx = testedGrid().transactions().txStart(conc, isolation)) {
                            res = cache.invokeAsync(key, INC_ENTRY_PROC_USER_OBJ, dataMode).get();

                            tx.commit();
                        }

                        // TODO IGNITE-2953: uncomment the following assert when the issue will be fixed.
//                              assertEquals(value(0), res);

                        assertEquals(value(1), deserializeBinary(cache.getAsync(key).get()));

                        try (Transaction tx = testedGrid().transactions().txStart(conc, isolation)) {
                            assertTrue((Boolean)cache.removeAsync(key).get());

                            tx.commit();
                        }
                    }
                }
                finally {
                    interceptorBinaryObjExp = true;
                }
            }
        }, PLANE_OBJECT, SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("serial")
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-11884")
    @Test
    public void testInvokeAll() throws Exception {
        runInAllDataModes(new TestRunnable() {
            @Override public void run() throws Exception {
                final IgniteCache cache = jcache().withKeepBinary();

                Set keys = new LinkedHashSet() {{
                    for (int i = 0; i < CNT; i++)
                        add(key(i));
                }};

                Map<Object, EntryProcessorResult<Object>> resMap = cache.invokeAll(keys, NOOP_ENTRY_PROC);

                for (Map.Entry<Object, EntryProcessorResult<Object>> e : resMap.entrySet()) {
                    assertTrue("Key:" + e.getKey(), e.getKey() instanceof BinaryObject);

                    assertNull(e.getValue().get());
                }

                resMap = cache.invokeAll(keys, INC_ENTRY_PROC_BINARY_OBJ, dataMode);

                checkInvokeAllResult(cache, resMap, null, value(0), true);

                resMap = cache.invokeAll(keys, INC_ENTRY_PROC_BINARY_OBJ, dataMode);

                checkInvokeAllResult(cache, resMap, value(0), value(1), true);

                cache.removeAll(keys);

                // TODO IGNITE-2973: should be always false.
                interceptorBinaryObjExp = atomicityMode() == TRANSACTIONAL;

                try {
                    resMap = cache.invokeAll(keys, INC_ENTRY_PROC_USER_OBJ, dataMode);

                    checkInvokeAllResult(cache, resMap, null, value(0), false);

                    resMap = cache.invokeAll(keys, INC_ENTRY_PROC_USER_OBJ, dataMode);

                    checkInvokeAllResult(cache, resMap, value(0), value(1), false);
                }
                finally {
                    interceptorBinaryObjExp = true;
                }
            }
        }, PLANE_OBJECT, SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInvokeAllTx() throws Exception {
        if (!txShouldBeUsed())
            return;

        for (TransactionConcurrency conc : TransactionConcurrency.values()) {
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                checkInvokeAllTx(conc, isolation);

                jcache().removeAll();
            }
        }
    }

    /**
     * @param conc Concurrency.
     * @param isol Isolation.
     * @throws Exception If failed.
     */
    @SuppressWarnings("serial")
    private void checkInvokeAllTx(final TransactionConcurrency conc, final TransactionIsolation isol) throws Exception {
        if (!txShouldBeUsed())
            return;

        runInAllDataModes(new TestRunnable() {
            @Override public void run() throws Exception {
                final IgniteCache cache = jcache().withKeepBinary();

                final Set keys = new LinkedHashSet() {{
                    for (int i = 0; i < CNT; i++)
                        add(key(i));
                }};

                try (Transaction tx = testedGrid().transactions().txStart(conc, isol)) {
                    Map<Object, EntryProcessorResult<Object>> resMap = cache.invokeAll(keys, NOOP_ENTRY_PROC);

                    for (Map.Entry<Object, EntryProcessorResult<Object>> e : resMap.entrySet()) {
                        assertTrue("Key:" + e.getKey(), e.getKey() instanceof BinaryObject);

                        assertNull(e.getValue().get());
                    }

                    tx.commit();
                }

                Map<Object, EntryProcessorResult<Object>> resMap;

                try (Transaction tx = testedGrid().transactions().txStart(conc, isol)) {
                    resMap = cache.invokeAll(keys, INC_ENTRY_PROC_BINARY_OBJ, dataMode);

                    tx.commit();
                }

                checkInvokeAllResult(cache, resMap, null, value(0), true);

                try (Transaction tx = testedGrid().transactions().txStart(conc, isol)) {
                    resMap = cache.invokeAll(keys, INC_ENTRY_PROC_BINARY_OBJ, dataMode);

                    tx.commit();
                }

                checkInvokeAllResult(cache, resMap, value(0), value(1), true);

                try (Transaction tx = testedGrid().transactions().txStart(conc, isol)) {
                    cache.removeAll(keys);

                    tx.commit();
                }

                // TODO IGNITE-2973: should be always false.
                interceptorBinaryObjExp = atomicityMode() == TRANSACTIONAL;

                try {
                    try (Transaction tx = testedGrid().transactions().txStart(conc, isol)) {
                        resMap = cache.invokeAll(keys, INC_ENTRY_PROC_USER_OBJ, dataMode);

                        tx.commit();
                    }

                    checkInvokeAllResult(cache, resMap, null, value(0), false);

                    try (Transaction tx = testedGrid().transactions().txStart(conc, isol)) {
                        resMap = cache.invokeAll(keys, INC_ENTRY_PROC_USER_OBJ, dataMode);

                        tx.commit();
                    }

                    checkInvokeAllResult(cache, resMap, value(0), value(1), false);

                    try (Transaction tx = testedGrid().transactions().txStart(conc, isol)) {
                        cache.removeAll(keys);

                        tx.commit();
                    }
                }
                finally {
                    interceptorBinaryObjExp = true;
                }
            }
        }, PLANE_OBJECT, SERIALIZABLE);
    }

    /**
     * @param cache Cache.
     * @param resMap Result map.
     * @param expRes Expected result.
     * @param cacheVal Expected cache value for key.
     * @param deserializeRes Deseriallize result flag.
     */
    private void checkInvokeAllResult(IgniteCache cache, Map<Object, EntryProcessorResult<Object>> resMap,
        Object expRes, Object cacheVal, boolean deserializeRes) {
        for (Map.Entry<Object, EntryProcessorResult<Object>> e : resMap.entrySet()) {
            info("Key: " + e.getKey());

            assertTrue("Wrong key type, binary object expected: " + e.getKey(), e.getKey() instanceof BinaryObject);

            Object res = e.getValue().get();

            // TODO IGNITE-2953: delete the following if when the issue wiil be fixed.
            if (deserializeRes)
                assertEquals(expRes, deserializeRes ? deserializeBinary(res) : res);

            if (cache.get(e.getKey()) == null)
                cache.get(e.getKey());

            assertEquals(cacheVal, deserializeBinary(cache.get(e.getKey())));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("serial")
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-11884")
    @Test
    public void testInvokeAllAsync() throws Exception {
        runInAllDataModes(new TestRunnable() {
            @Override public void run() throws Exception {
                final IgniteCache cache = jcache().withKeepBinary();

                final Set keys = new LinkedHashSet() {{
                    for (int i = 0; i < CNT; i++)
                        add(key(i));
                }};

                Map<Object, EntryProcessorResult<Object>> resMap =
                    (Map<Object, EntryProcessorResult<Object>>)cache.invokeAllAsync(keys, NOOP_ENTRY_PROC).get();

                for (Map.Entry<Object, EntryProcessorResult<Object>> e : resMap.entrySet()) {
                    assertTrue("Wrong key type, binary object expected: " + e.getKey(), e.getKey() instanceof BinaryObject);

                    assertNull(e.getValue().get());
                }

                resMap = (Map<Object, EntryProcessorResult<Object>>)
                    cache.invokeAllAsync(keys, INC_ENTRY_PROC_BINARY_OBJ, dataMode).get();

                checkInvokeAllAsyncResult(cache, resMap, null, value(0), true);

                resMap = (Map<Object, EntryProcessorResult<Object>>)
                    cache.invokeAllAsync(keys, INC_ENTRY_PROC_BINARY_OBJ, dataMode).get();

                checkInvokeAllAsyncResult(cache, resMap, value(0), value(1), true);

                cache.removeAllAsync(keys).get();

                // TODO IGNITE-2973: should be always false.
                interceptorBinaryObjExp = atomicityMode() == TRANSACTIONAL;

                try {
                    resMap = (Map<Object, EntryProcessorResult<Object>>)
                        cache.invokeAllAsync(keys, INC_ENTRY_PROC_USER_OBJ, dataMode).get();

                    checkInvokeAllAsyncResult(cache, resMap, null, value(0), false);

                    resMap = (Map<Object, EntryProcessorResult<Object>>)
                        cache.invokeAllAsync(keys, INC_ENTRY_PROC_USER_OBJ, dataMode).get();

                    checkInvokeAllAsyncResult(cache, resMap, value(0), value(1), false);

                    cache.removeAllAsync(keys).get();
                }
                finally {
                    interceptorBinaryObjExp = true;
                }
            }
        }, PLANE_OBJECT, SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInvokeAllAsyncTx() throws Exception {
        if (!txShouldBeUsed())
            return;

        for (TransactionConcurrency conc : TransactionConcurrency.values()) {
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                checkInvokeAllAsycnTx(conc, isolation);

                jcache().removeAll();
            }
        }
    }

    /**
     *
     * @param conc Concurrency.
     * @param isolation Isolation.
     * @throws Exception If failed.
     */
    private void checkInvokeAllAsycnTx(final TransactionConcurrency conc, final TransactionIsolation isolation) throws Exception {
        runInAllDataModes(new TestRunnable() {
            @Override public void run() throws Exception {
                final IgniteCache cache = jcache().withKeepBinary();

                final Set keys = new LinkedHashSet() {{
                    for (int i = 0; i < CNT; i++)
                        add(key(i));
                }};

                Map<Object, EntryProcessorResult<Object>> resMap;

                try (Transaction tx = testedGrid().transactions().txStart(conc, isolation)) {
                    resMap = (Map<Object, EntryProcessorResult<Object>>)
                        cache.invokeAllAsync(keys, NOOP_ENTRY_PROC).get();

                    tx.commit();
                }

                for (Map.Entry<Object, EntryProcessorResult<Object>> e : resMap.entrySet()) {
                    assertTrue("Key:" + e.getKey(), e.getKey() instanceof BinaryObject);

                    assertNull(e.getValue().get());
                }

                try (Transaction tx = testedGrid().transactions().txStart(conc, isolation)) {
                    resMap = (Map<Object, EntryProcessorResult<Object>>)
                        cache.invokeAllAsync(keys, INC_ENTRY_PROC_BINARY_OBJ, dataMode).get();

                    tx.commit();
                }

                checkInvokeAllAsyncResult(cache, resMap, null, value(0), true);

                try (Transaction tx = testedGrid().transactions().txStart(conc, isolation)) {
                    resMap = (Map<Object, EntryProcessorResult<Object>>)
                        cache.invokeAllAsync(keys, INC_ENTRY_PROC_BINARY_OBJ, dataMode).get();

                    tx.commit();
                }

                checkInvokeAllAsyncResult(cache, resMap, value(0), value(1), true);

                try (Transaction tx = testedGrid().transactions().txStart(conc, isolation)) {
                    cache.removeAllAsync(keys).get();

                    tx.commit();
                }

                // TODO IGNITE-2973: should be always false.
                interceptorBinaryObjExp = atomicityMode() == TRANSACTIONAL;

                try {
                    try (Transaction tx = testedGrid().transactions().txStart(conc, isolation)) {
                        resMap = (Map<Object, EntryProcessorResult<Object>>)
                            cache.invokeAllAsync(keys, INC_ENTRY_PROC_USER_OBJ, dataMode).get();

                        tx.commit();
                    }

                    checkInvokeAllAsyncResult(cache, resMap, null, value(0), false);

                    try (Transaction tx = testedGrid().transactions().txStart(conc, isolation)) {
                        resMap = (Map<Object, EntryProcessorResult<Object>>)
                            cache.invokeAllAsync(keys, INC_ENTRY_PROC_USER_OBJ, dataMode).get();

                        tx.commit();
                    }

                    checkInvokeAllAsyncResult(cache, resMap, value(0), value(1), false);

                    try (Transaction tx = testedGrid().transactions().txStart(conc, isolation)) {
                        cache.removeAllAsync(keys).get();

                        tx.commit();
                    }
                }
                finally {
                    interceptorBinaryObjExp = true;
                }
            }
        }, PLANE_OBJECT, SERIALIZABLE);
    }

    /**
     * @param cache Cache.
     * @param resMap Result map.
     * @param expRes Expected result.
     * @param cacheVal Expected cache value for key.
     * @param deserializeRes Deseriallize result flag.
     */
    private void checkInvokeAllAsyncResult(IgniteCache cache, Map<Object, EntryProcessorResult<Object>> resMap,
        Object expRes, Object cacheVal, boolean deserializeRes) {
        for (Map.Entry<Object, EntryProcessorResult<Object>> e : resMap.entrySet()) {
            info("Key: " + e.getKey());

            assertTrue("Wrong key type, binary object expected: " + e.getKey(), e.getKey() instanceof BinaryObject);

            Object res = e.getValue().get();

            // TODO IGNITE-2953: delete the following if when the issue wiil be fixed.
            if (deserializeRes)
                assertEquals(expRes, deserializeRes ? deserializeBinary(res) : res);

            assertEquals(cacheVal, deserializeBinary(cache.getAsync(e.getKey()).get()));
        }
    }

    /**
     * @param val Value
     * @return User object.
     */
    private static Object deserializeBinary(Object val) {
        assertTrue("Val: " + val, val instanceof BinaryObject);

        return ((BinaryObject)val).deserialize();
    }

    /**
     * @param task Task.
     * @throws Exception If failed.
     */
    protected void runInAllTxModes(TestRunnable task) throws Exception {
        info("Executing implicite tx");

        task.run();

        if (txShouldBeUsed()) {
            for (TransactionConcurrency conc : TransactionConcurrency.values()) {
                for (TransactionIsolation isolation : TransactionIsolation.values()) {
                    try (Transaction tx = testedGrid().transactions().txStart(conc, isolation)) {
                        info("Executing explicite tx [isolation" + isolation + ", concurrency=" + conc + "]");

                        task.run();

                        tx.commit();
                    }
                }
            }
        }
    }
}
