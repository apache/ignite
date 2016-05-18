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
import org.apache.ignite.testframework.junits.IgniteCacheConfigVariationsAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

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
    @SuppressWarnings("unchecked")
    @Override protected CacheConfiguration cacheConfiguration() {
        CacheConfiguration cc = super.cacheConfiguration();

        cc.setStoreKeepBinary(true);

        return cc;
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("serial")
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
    public void testRemovePutGetAsync() throws Exception {
        runInAllDataModes(new TestRunnable() {
            @Override public void run() throws Exception {
                final IgniteCache cache = jcache().withKeepBinary().withAsync();

                final Set keys = new LinkedHashSet() {{
                    for (int i = 0; i < CNT; i++)
                        add(key(i));
                }};

                runInAllTxModes(new TestRunnable() {
                    @Override public void run() throws Exception {
                        for (Object key : keys) {
                            cache.remove(key);

                            cache.future().get();
                        }
                    }
                });

                runInAllTxModes(new TestRunnable() {
                    @Override public void run() throws Exception {
                        for (Object key : keys) {
                            cache.get(key);
                            assertNull(cache.future().get());

                            cache.getEntry(key);
                            assertNull(cache.future().get());
                        }
                    }
                });


                for (final Object key : keys) {
                    runInAllTxModes(new TestRunnable() {
                        @Override public void run() throws Exception {
                            Object val = value(valueOf(key));

                            cache.put(key, val);

                            cache.future().get();

                            cache.get(key);
                            BinaryObject retVal = (BinaryObject)cache.future().get();

                            assertEquals(val, retVal.deserialize());

                            cache.getEntry(key);
                            CacheEntry<BinaryObject, BinaryObject> e = (CacheEntry<BinaryObject, BinaryObject>)cache.future().get();

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
    public void testPutAllGetAllAsync() throws Exception {
        runInAllDataModes(new TestRunnable() {
            @Override public void run() throws Exception {
                final IgniteCache cache = jcache().withKeepBinary().withAsync();

                final Set keys = new LinkedHashSet() {{
                    for (int i = 0; i < CNT; i++)
                        add(key(i));
                }};

                runInAllTxModes(new TestRunnable() {
                    @Override public void run() throws Exception {
                        cache.getAll(keys);
                        Map res = (Map)cache.future().get();

                        for (Object val : res.values())
                            assertNull(val);
                    }
                });

                runInAllTxModes(new TestRunnable() {
                    @Override public void run() throws Exception {
                        cache.<CacheEntry>getEntries(keys);

                        Collection<CacheEntry> entries = (Collection<CacheEntry>)cache.future().get();

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

                        cache.future().get();

                        cache.getAll(keys);
                        Set<Map.Entry<BinaryObject, BinaryObject>> set = ((Map)cache.future().get()).entrySet();

                        for (Map.Entry<BinaryObject, BinaryObject> e : set) {
                            Object expVal = value(valueOf(e.getKey().deserialize()));

                            assertEquals(expVal, e.getValue().deserialize());
                        }

                        cache.getEntries(keys);

                        Collection<CacheEntry<BinaryObject, BinaryObject>> entries =
                            (Collection<CacheEntry<BinaryObject, BinaryObject>>)cache.future().get();

                        for (CacheEntry<BinaryObject, BinaryObject> e : entries) {
                            assertTrue(e.getKey() instanceof BinaryObject);

                            Object expVal = value(valueOf(e.getKey().deserialize()));

                            assertEquals(expVal, e.getValue().deserialize());
                        }

                        cache.removeAll(keys);

                        cache.future().get();
                    }
                });
            }
        }, PLANE_OBJECT, SERIALIZABLE);
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("serial")
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
    public void testInvokeTx() throws Exception {
        if (!txShouldBeUsed())
            return;

        for (TransactionConcurrency conc : TransactionConcurrency.values()) {
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                // TODO IGNITE-2971: delete this if when the issue will be fixed.
                if (conc == TransactionConcurrency.OPTIMISTIC && isolation == TransactionIsolation.SERIALIZABLE)
                    continue;

                info(">>>>> Executing test using explicite txs [concurrency=" + conc + ", isolation=" + isolation + "]");

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
    public void testInvokeAsync() throws Exception {
        runInAllDataModes(new TestRunnable() {
            @Override public void run() throws Exception {
                final IgniteCache cache = jcache().withKeepBinary().withAsync();

                Set keys = new LinkedHashSet() {{
                    for (int i = 0; i < CNT; i++)
                        add(key(i));
                }};

                for (final Object key : keys) {
                    cache.invoke(key, NOOP_ENTRY_PROC);

                    Object res = cache.future().get();

                    assertNull(res);

                    cache.get(key);

                    assertNull(cache.future().get());
                }

                for (final Object key : keys) {
                    cache.invoke(key, INC_ENTRY_PROC_BINARY_OBJ, dataMode);

                    Object res = cache.future().get();

                    assertNull(res);

                    cache.get(key);

                    assertEquals(value(0), deserializeBinary(cache.future().get()));

                    cache.invoke(key, INC_ENTRY_PROC_BINARY_OBJ, dataMode);

                    res = cache.future().get();

                    assertEquals(value(0), deserializeBinary(res));

                    cache.get(key);

                    assertEquals(value(1), deserializeBinary(cache.future().get()));

                    cache.remove(key);

                    assertTrue((Boolean)cache.future().get());
                }

                // TODO IGNITE-2973: should be always false.
                interceptorBinaryObjExp = atomicityMode() == TRANSACTIONAL;

                try {
                    for (final Object key : keys) {
                        cache.invoke(key, INC_ENTRY_PROC_USER_OBJ, dataMode);

                        Object res = cache.future().get();

                        assertNull(res);

                        cache.get(key);

                        assertEquals(value(0), deserializeBinary(cache.future().get()));

                        cache.invoke(key, INC_ENTRY_PROC_USER_OBJ, dataMode);

                        res = cache.future().get();

                        // TODO IGNITE-2953: uncomment the following assert when the issue will be fixed.
//                              assertEquals(value(0), res);

                        cache.get(key);

                        assertEquals(value(1), deserializeBinary(cache.future().get()));

                        cache.remove(key);

                        assertTrue((Boolean)cache.future().get());
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
    public void testInvokeAsyncTx() throws Exception {
        if (!txShouldBeUsed())
            return;

        for (TransactionConcurrency conc : TransactionConcurrency.values()) {
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                // TODO IGNITE-2971: delete this if when the issue will be fixed.
                if (conc == TransactionConcurrency.OPTIMISTIC && isolation == TransactionIsolation.SERIALIZABLE)
                    continue;

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
                final IgniteCache cache = jcache().withKeepBinary().withAsync();

                Set keys = new LinkedHashSet() {{
                    for (int i = 0; i < CNT; i++)
                        add(key(i));
                }};

                try (Transaction tx = testedGrid().transactions().txStart(conc, isolation)) {
                    for (final Object key : keys) {
                        cache.invoke(key, NOOP_ENTRY_PROC);

                        Object res = cache.future().get();

                        assertNull(res);

                        cache.get(key);

                        assertNull(cache.future().get());
                    }

                    tx.commit();
                }

                for (final Object key : keys) {
                    Object res;

                    try (Transaction tx = testedGrid().transactions().txStart(conc, isolation)) {
                        cache.invoke(key, INC_ENTRY_PROC_BINARY_OBJ, dataMode);

                        res = cache.future().get();

                        tx.commit();
                    }

                    assertNull(res);

                    cache.get(key);

                    assertEquals(value(0), deserializeBinary(cache.future().get()));

                    try (Transaction tx = testedGrid().transactions().txStart(conc, isolation)) {
                        cache.invoke(key, INC_ENTRY_PROC_BINARY_OBJ, dataMode);

                        tx.commit();
                    }

                    res = cache.future().get();

                    assertEquals(value(0), deserializeBinary(res));

                    cache.get(key);

                    assertEquals(value(1), deserializeBinary(cache.future().get()));

                    try (Transaction tx = testedGrid().transactions().txStart(conc, isolation)) {
                        cache.remove(key);

                        assertTrue((Boolean)cache.future().get());

                        tx.commit();
                    }
                }

                // TODO IGNITE-2973: should be always false.
                interceptorBinaryObjExp = atomicityMode() == TRANSACTIONAL;

                try {
                    for (final Object key : keys) {
                        Object res;

                        try (Transaction tx = testedGrid().transactions().txStart(conc, isolation)) {
                            cache.invoke(key, INC_ENTRY_PROC_USER_OBJ, dataMode);

                            res = cache.future().get();

                            tx.commit();
                        }

                        assertNull(res);

                        cache.get(key);

                        assertEquals(value(0), deserializeBinary(cache.future().get()));

                        try (Transaction tx = testedGrid().transactions().txStart(conc, isolation)) {
                            cache.invoke(key, INC_ENTRY_PROC_USER_OBJ, dataMode);

                            res = cache.future().get();

                            tx.commit();
                        }

                        // TODO IGNITE-2953: uncomment the following assert when the issue will be fixed.
//                              assertEquals(value(0), res);

                        cache.get(key);

                        assertEquals(value(1), deserializeBinary(cache.future().get()));

                        try (Transaction tx = testedGrid().transactions().txStart(conc, isolation)) {
                            cache.remove(key);

                            assertTrue((Boolean)cache.future().get());

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
    @SuppressWarnings("serial")
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
    public void testInvokeAllAsync() throws Exception {
        runInAllDataModes(new TestRunnable() {
            @Override public void run() throws Exception {
                final IgniteCache cache = jcache().withKeepBinary().withAsync();

                final Set keys = new LinkedHashSet() {{
                    for (int i = 0; i < CNT; i++)
                        add(key(i));
                }};

                cache.invokeAll(keys, NOOP_ENTRY_PROC);

                Map<Object, EntryProcessorResult<Object>> resMap =
                    (Map<Object, EntryProcessorResult<Object>>)cache.future().get();

                for (Map.Entry<Object, EntryProcessorResult<Object>> e : resMap.entrySet()) {
                    assertTrue("Wrong key type, binary object expected: " + e.getKey(), e.getKey() instanceof BinaryObject);

                    assertNull(e.getValue().get());
                }

                cache.invokeAll(keys, INC_ENTRY_PROC_BINARY_OBJ, dataMode);

                resMap = (Map<Object, EntryProcessorResult<Object>>)cache.future().get();

                checkInvokeAllAsyncResult(cache, resMap, null, value(0), true);

                cache.invokeAll(keys, INC_ENTRY_PROC_BINARY_OBJ, dataMode);

                resMap = (Map<Object, EntryProcessorResult<Object>>)cache.future().get();

                checkInvokeAllAsyncResult(cache, resMap, value(0), value(1), true);

                cache.removeAll(keys);

                cache.future().get();

                // TODO IGNITE-2973: should be always false.
                interceptorBinaryObjExp = atomicityMode() == TRANSACTIONAL;

                try {
                    cache.invokeAll(keys, INC_ENTRY_PROC_USER_OBJ, dataMode);

                    resMap = (Map<Object, EntryProcessorResult<Object>>)cache.future().get();

                    checkInvokeAllAsyncResult(cache, resMap, null, value(0), false);

                    cache.invokeAll(keys, INC_ENTRY_PROC_USER_OBJ, dataMode);

                    resMap = (Map<Object, EntryProcessorResult<Object>>)cache.future().get();

                    checkInvokeAllAsyncResult(cache, resMap, value(0), value(1), false);

                    cache.removeAll(keys);

                    cache.future().get();
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
     * @throws Exception
     */
    private void checkInvokeAllAsycnTx(final TransactionConcurrency conc, final TransactionIsolation isolation) throws Exception {
        runInAllDataModes(new TestRunnable() {
            @Override public void run() throws Exception {
                final IgniteCache cache = jcache().withKeepBinary().withAsync();

                final Set keys = new LinkedHashSet() {{
                    for (int i = 0; i < CNT; i++)
                        add(key(i));
                }};

                Map<Object, EntryProcessorResult<Object>> resMap;

                try (Transaction tx = testedGrid().transactions().txStart(conc, isolation)) {
                    cache.invokeAll(keys, NOOP_ENTRY_PROC);

                    resMap = (Map<Object, EntryProcessorResult<Object>>)cache.future().get();

                    tx.commit();
                }

                for (Map.Entry<Object, EntryProcessorResult<Object>> e : resMap.entrySet()) {
                    assertTrue("Key:" + e.getKey(), e.getKey() instanceof BinaryObject);

                    assertNull(e.getValue().get());
                }

                try (Transaction tx = testedGrid().transactions().txStart(conc, isolation)) {
                    cache.invokeAll(keys, INC_ENTRY_PROC_BINARY_OBJ, dataMode);

                    resMap = (Map<Object, EntryProcessorResult<Object>>)cache.future().get();

                    tx.commit();
                }

                checkInvokeAllAsyncResult(cache, resMap, null, value(0), true);

                try (Transaction tx = testedGrid().transactions().txStart(conc, isolation)) {
                    cache.invokeAll(keys, INC_ENTRY_PROC_BINARY_OBJ, dataMode);

                    resMap = (Map<Object, EntryProcessorResult<Object>>)cache.future().get();

                    tx.commit();
                }

                checkInvokeAllAsyncResult(cache, resMap, value(0), value(1), true);

                try (Transaction tx = testedGrid().transactions().txStart(conc, isolation)) {
                    cache.removeAll(keys);

                    cache.future().get();

                    tx.commit();
                }

                // TODO IGNITE-2973: should be always false.
                interceptorBinaryObjExp = atomicityMode() == TRANSACTIONAL;

                try {
                    try (Transaction tx = testedGrid().transactions().txStart(conc, isolation)) {
                        cache.invokeAll(keys, INC_ENTRY_PROC_USER_OBJ, dataMode);

                        resMap = (Map<Object, EntryProcessorResult<Object>>)cache.future().get();

                        tx.commit();
                    }

                    checkInvokeAllAsyncResult(cache, resMap, null, value(0), false);

                    try (Transaction tx = testedGrid().transactions().txStart(conc, isolation)) {
                        cache.invokeAll(keys, INC_ENTRY_PROC_USER_OBJ, dataMode);

                        resMap = (Map<Object, EntryProcessorResult<Object>>)cache.future().get();

                        tx.commit();
                    }

                    checkInvokeAllAsyncResult(cache, resMap, value(0), value(1), false);

                    try (Transaction tx = testedGrid().transactions().txStart(conc, isolation)) {
                        cache.removeAll(keys);

                        cache.future().get();

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

            cache.get(e.getKey());

            assertEquals(cacheVal, deserializeBinary(cache.future().get()));
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
