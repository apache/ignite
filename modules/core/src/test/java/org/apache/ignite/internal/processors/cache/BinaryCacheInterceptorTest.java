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
import javax.cache.Cache;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CacheInterceptor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.binary.BinaryObjectOffheapImpl;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.junits.IgniteCacheConfigVariationsAbstractTest;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
@SuppressWarnings("unchecked")
public class BinaryCacheInterceptorTest extends IgniteCacheConfigVariationsAbstractTest {
    /** */
    private static volatile boolean validate;

    /** */
    private static volatile boolean binaryObjExp = true;

    /** */
    public static final int CNT = 10;

    /** */
    public static final CacheEntryProcessor NOOP_ENTRY_PROC = new CacheEntryProcessor() {
        @Override public Object process(MutableEntry entry, Object... arguments) throws EntryProcessorException {
            return null;
        }
    };

    /** */
    public static final CacheEntryProcessor INC_ENTRY_PROC_RET_USER_OBJ = new CacheEntryProcessor() {
        @Override public Object process(MutableEntry entry, Object... arguments) throws EntryProcessorException {
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
    public static final CacheEntryProcessor INC_ENTRY_PROC_RET_BINARY_OBJ = new CacheEntryProcessor() {
        @Override public Object process(MutableEntry entry, Object... arguments) throws EntryProcessorException {
            assertFalse(entry.getKey() instanceof BinaryObjectOffheapImpl);

            Object val = entry.getValue();

            int valId = 0;

            if (val != null) {
                assertTrue(val instanceof BinaryObject);

                assertFalse(val instanceof BinaryObjectOffheapImpl);

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

        cc.setInterceptor(new TestInterceptor());
        cc.setStoreKeepBinary(true);

        return cc;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        dataMode = DataMode.PLANE_OBJECT;

        validate = true;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        validate = false;

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("serial")
    public void testRemovePutGet() throws Exception {
        final IgniteCache cache = jcache().withKeepBinary();

        Set keys = new LinkedHashSet() {{
            for (int i = 0; i < CNT; i++)
                add(key(i));
        }};

        for (Object key : keys)
            cache.remove(key);

        for (Object key : keys) {
            assertNull(cache.get(key));
            assertNull(cache.getEntry(key));
        }

        for (Object key : keys) {
            Object val = value(valueOf(key));

            cache.put(key, val);

            BinaryObject retVal = (BinaryObject)cache.get(key);

            assertEquals(val, retVal.deserialize());

            CacheEntry<BinaryObject, BinaryObject> entry = cache.getEntry(key);

            // TODO fix it or not?
//            assertTrue(entry.getKey() instanceof BinaryObject);

            assertEquals(val, entry.getValue().deserialize());
        }
    }
    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("serial")
    public void testPutAllGetAll() throws Exception {
        final IgniteCache cache = jcache().withKeepBinary();

        final Set keys = new LinkedHashSet() {{
            for (int i = 0; i < CNT; i++)
                add(key(i));
        }};

        for (Object val : cache.getAll(keys).values())
            assertNull(val);

        Collection<CacheEntry> entries = cache.<CacheEntry>getEntries(keys);

        for (CacheEntry e : entries)
            assertNull(e.getValue());

        Map keyValMap = new LinkedHashMap(){{
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

        entries = cache.getEntries(keys);

        for (CacheEntry<BinaryObject, BinaryObject> e : entries) {
            assertTrue(e.getKey() instanceof BinaryObject);

            Object expVal = value(valueOf(e.getKey().deserialize()));

            assertEquals(expVal, e.getValue().deserialize());
        }

        cache.removeAll(keys);
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("serial")
    public void testInvoke() throws Exception {
        final IgniteCache cache = jcache().withKeepBinary();

        Set keys = new LinkedHashSet() {{
            for (int i = 0; i < CNT; i++)
                add(key(i));
        }};

        for (Object key : keys) {
            Object res = cache.invoke(key, NOOP_ENTRY_PROC);

            assertNull(res);

            assertNull(cache.get(key));
        }

        for (Object key : keys) {
            Object res = cache.invoke(key, INC_ENTRY_PROC_RET_BINARY_OBJ, dataMode);

            assertNull(res);
            assertEquals(value(0), deserializeBinary(cache.get(key)));

            res = cache.invoke(key, INC_ENTRY_PROC_RET_BINARY_OBJ, dataMode);

            assertEquals(value(0), deserializeBinary(res));
            assertEquals(value(1), deserializeBinary(cache.get(key)));
        }

        for (Object key : keys)
            cache.remove(key);

        // TODO. beforePut gets newVal as usrObj and then afterPut gets e.getVal as usrObj
//        binaryObjectExpected = false;
//
//        try {
//            for (Object key : keys) {
//                Object res = cache.invoke(key, INC_ENTRY_PROC_RET_USER_OBJ, dataMode);
//
//                assertNull(res);
//                assertEquals(value(0), deserializeBinary(cache.get(key)));
//
//                res = cache.invoke(key, INC_ENTRY_PROC_RET_USER_OBJ, dataMode);
//
//                assertEquals(value(0), res);
//                assertEquals(value(1), deserializeBinary(cache.get(key)));
//            }
//        }
//        finally {
//            binaryObjectExpected = true;
//        }
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("serial")
    public void testInvokeAll() throws Exception {
        final IgniteCache cache = jcache().withKeepBinary();

        Set keys = new LinkedHashSet() {{
            for (int i = 0; i < CNT; i++)
                add(key(i));
        }};

        for (Object key : keys) {
            Object res = cache.invoke(key, NOOP_ENTRY_PROC);

            assertNull(res);

            assertNull(cache.get(key));
        }

        Map<BinaryObject, EntryProcessorResult<BinaryObject>> resMap = cache.invokeAll(keys, INC_ENTRY_PROC_RET_BINARY_OBJ, dataMode);

        for (Map.Entry<BinaryObject, EntryProcessorResult<BinaryObject>> e : resMap.entrySet()) {
            assertNull(e.getValue().get());

            assertEquals(value(0), deserializeBinary(cache.get(e.getKey())));
        }

        resMap = cache.invokeAll(keys, INC_ENTRY_PROC_RET_BINARY_OBJ, dataMode);

        for (Map.Entry<BinaryObject, EntryProcessorResult<BinaryObject>> e : resMap.entrySet()) {
            assertEquals(value(0), deserializeBinary(e.getValue().get()));

            assertEquals(value(1), deserializeBinary(cache.get(e.getKey())));
        }

        cache.removeAll(keys);

        // TODO. Below we get inconsistent results: sometimes newVal is userObj, sometimes it's BinaryObj.
//        binaryObjExp = false;
//
//        try {
//            resMap = cache.invokeAll(keys, INC_ENTRY_PROC_RET_USER_OBJ, dataMode);
//
//            for (Map.Entry<BinaryObject, EntryProcessorResult<BinaryObject>> e : resMap.entrySet()) {
//                assertNull(e.getValue().get());
//
//                assertEquals(value(0), cache.get(e.getKey()));
//            }
//
//            resMap = cache.invokeAll(keys, INC_ENTRY_PROC_RET_USER_OBJ, dataMode);
//
//            for (Map.Entry<BinaryObject, EntryProcessorResult<BinaryObject>> e : resMap.entrySet()) {
//                assertEquals(value(0), e.getValue().get());
//
//                assertEquals(value(1), cache.get(e.getKey()));
//            }
//        }
//        finally {
//            binaryObjExp = true;
//        }
    }

    /**
     * @param val Value
     * @return User object.
     */
    private static Object deserializeBinary(Object val) {
        assertTrue(val instanceof BinaryObject);

        return ((BinaryObject)val).deserialize();
    }

    /**
     *
     */
    private static class TestInterceptor<K, V> implements CacheInterceptor<K, V> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Nullable @Override public V onGet(K key, V val) {
            if (validate)
                assertTrue("Val: " + val, val == null || val instanceof BinaryObject);

            return val;
        }

        /** {@inheritDoc} */
        @Nullable @Override public V onBeforePut(Cache.Entry<K, V> e, V newVal) {
            if (validate) {
                validateEntry(e);

                if (newVal != null) {
                    assertEquals("NewVal: " + newVal, binaryObjExp, newVal instanceof BinaryObject);

                    assertFalse(e.getKey() instanceof BinaryObjectOffheapImpl);
                }
            }

            return newVal;
        }

        /** {@inheritDoc} */
        @Override public void onAfterPut(Cache.Entry<K, V> entry) {
            validateEntry(entry);
        }

        /** {@inheritDoc} */
        @Nullable @Override public IgniteBiTuple<Boolean, V> onBeforeRemove(Cache.Entry<K, V> entry) {
            validateEntry(entry);

            return new IgniteBiTuple<>(false, entry.getValue());
        }

        /** {@inheritDoc} */
        @Override public void onAfterRemove(Cache.Entry<K, V> entry) {
            validateEntry(entry);
        }

        /**
         * @param e Value.
         */
        private void validateEntry(Cache.Entry<K, V> e) {
            assertNotNull(e);
            assertNotNull(e.getKey());

            assertFalse(e.getKey() instanceof BinaryObjectOffheapImpl);
            assertFalse(e.getValue() instanceof BinaryObjectOffheapImpl);

            if (validate) {
                assertTrue("Key: " + e.getKey(), e.getKey() instanceof BinaryObject);

                if (e.getValue() != null)
                    assertTrue("Val: " + e.getValue(), e.getValue() instanceof BinaryObject);
            }
        }
    }
}
