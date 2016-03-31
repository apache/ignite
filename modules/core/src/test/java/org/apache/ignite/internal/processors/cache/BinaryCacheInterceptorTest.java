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

import java.util.LinkedHashSet;
import java.util.Set;
import javax.cache.Cache;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CacheInterceptor;
import org.apache.ignite.configuration.CacheConfiguration;
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
    private static volatile boolean binaryObjectExpected = true;

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
    public void testPutGet() throws Exception {
        final IgniteCache cache = jcache().withKeepBinary();

        Set keys = new LinkedHashSet() {{
            for (int i = 0; i < CNT; i++)
                add(key(i));
        }};

        Set unknownKeys = new LinkedHashSet() {{
            for (int i = CNT; i < 2 * CNT; i++)
                add(key(i));
        }};

        for (Object key : unknownKeys)
            assertNull(cache.get(key));

        for (Object key : keys) {
            Object val = value(valueOf(key));

            cache.put(key, val);

            BinaryObject retVal = (BinaryObject)cache.get(key);

            assertEquals(val, retVal.deserialize());
        }
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

        cache.removeAll(keys);

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
    public void _testRest() throws Exception {
        final IgniteCache cache = jcache().withKeepBinary();

        Set keys = new LinkedHashSet() {{
            for (int i = 0; i < CNT; i++)
                add(key(i));
        }};

        Set unknownKeys = new LinkedHashSet() {{
            for (int i = CNT; i < 2 * CNT; i++)
                add(key(i));
        }};

        Set allKeys = new LinkedHashSet();
        allKeys.addAll(keys);
        allKeys.addAll(unknownKeys);

        for (Object key : keys) {
            Object val = value(valueOf(key));

            cache.put(key, val);

            BinaryObject retVal = (BinaryObject)cache.get(key);

            assertEquals(val, retVal.deserialize());
        }

        for (Object key : allKeys) {
            cache.get(key);
            cache.getEntry(key);
            cache.invoke(key, NOOP_ENTRY_PROC);
            cache.invoke(key, INC_ENTRY_PROC_RET_USER_OBJ, dataMode);
        }

        cache.getAll(allKeys);
        cache.getEntries(allKeys);
        cache.getAllOutTx(allKeys);
        cache.invokeAll(allKeys, NOOP_ENTRY_PROC);
        cache.invokeAll(allKeys, INC_ENTRY_PROC_RET_USER_OBJ, dataMode);

        for (Object key : allKeys)
            cache.remove(key);
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
                validateOldVal(e.getValue());

                if (newVal != null)
                    assertEquals("NewVal: " + newVal, binaryObjectExpected, newVal instanceof BinaryObject);
            }

            return newVal;
        }

        /** {@inheritDoc} */
        @Override public void onAfterPut(Cache.Entry<K, V> entry) {
            validateOldVal(entry.getValue());
        }

        /** {@inheritDoc} */
        @Nullable @Override public IgniteBiTuple<Boolean, V> onBeforeRemove(Cache.Entry<K, V> entry) {
            V val = entry.getValue();

            validateOldVal(val);

            return new IgniteBiTuple<>(false, val);
        }

        /** {@inheritDoc} */
        @Override public void onAfterRemove(Cache.Entry<K, V> entry) {
            validateOldVal(entry.getValue());
        }

        /**
         * @param val Value.
         */
        private void validateOldVal(V val) {
            if (validate && val != null)
                assertEquals("Val: " + val, true, val instanceof BinaryObject);
        }
    }
}
