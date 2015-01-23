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

package org.apache.ignite.internal.processors.cache.local;

import org.apache.ignite.cache.*;
import org.apache.ignite.internal.processors.cache.*;

import java.io.*;
import java.util.*;

import static org.apache.ignite.cache.GridCacheMode.*;

/**
 * Tests for local cache iterators.
 */
public class GridCacheLocalIteratorsSelfTest extends GridCacheAbstractIteratorsSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheMode cacheMode() {
        return LOCAL;
    }

    /** {@inheritDoc} */
    @Override protected int entryCount() {
        return 1000;
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheIteratorSerialization() throws Exception {
        testIteratorSerialization(cache().iterator(), entryCount());
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheProjectionIteratorSerialization() throws Exception {
        testIteratorSerialization(cache().projection(lt50).iterator(), 50);
    }

    /**
     * @throws Exception If failed.
     */
    public void testEntrySetIteratorSerialization() throws Exception {
        testIteratorSerialization(cache().entrySet().iterator(), entryCount());
    }

    /**
     * @throws Exception If failed.
     */
    public void testFilteredEntrySetIteratorSerialization() throws Exception {
        testIteratorSerialization(cache().projection(lt50).entrySet().iterator(), 50);
    }

    /**
     * @throws Exception If failed.
     */
    public void testKeySetIteratorSerialization() throws Exception {
        testIteratorSerialization(cache().keySet().iterator(), entryCount());
    }

    /**
     * @throws Exception If failed.
     */
    public void testFilteredKeySetIteratorSerialization() throws Exception {
        testIteratorSerialization(cache().projection(lt50).keySet().iterator(), 50);
    }

    /**
     * @throws Exception If failed.
     */
    public void testValuesIteratorSerialization() throws Exception {
        testIteratorSerialization(cache().values().iterator(), entryCount());
    }

    /**
     * @throws Exception If failed.
     */
    public void testFilteredValuesIteratorSerialization() throws Exception {
        testIteratorSerialization(cache().projection(lt50).values().iterator(), 50);
    }

    /**
     * @param it Iterator.
     * @param bound Value bound.
     * @throws Exception If failed.
     */
    private void testIteratorSerialization(Iterator<?> it, int bound) throws Exception {
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();

        try (ObjectOutputStream out = new ObjectOutputStream(byteOut)) {
            out.writeObject(it);
        }

        byte[] bytes = byteOut.toByteArray();

        ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes));

        Iterator<?> it0 = (Iterator<?>)in.readObject();

        int cnt = 0;

        while (it0.hasNext()) {
            Object obj = it0.next();

            if (obj instanceof GridCacheEntry)
                checkEntry((GridCacheEntry<String, Integer>)obj, bound);
            else if (obj instanceof String)
                checkKey((String)obj);
            else if (obj instanceof Integer)
                checkValue((Integer)obj, bound);
            else
                assert false : "Wrong type.";

            cnt++;
        }

        assert cnt == bound;
    }

    /**
     * @param entry Entry.
     * @param bound Value bound.
     * @throws Exception If failed.
     */
    private void checkEntry(GridCacheEntry<String, Integer> entry, int bound) throws Exception {
        assert entry != null;

        checkKey(entry.getKey());
        checkValue(entry.getValue(), bound);
        checkValue(entry.get(), bound);
    }

    /**
     * @param key Key.
     * @throws Exception If failed.
     */
    private void checkKey(String key) throws Exception {
        assert key != null;
        assert key.contains(KEY_PREFIX);
    }

    /**
     * @param value Value.
     * @param bound Value bound.
     * @throws Exception If failed.
     */
    private void checkValue(Integer value, int bound) throws Exception {
        assert value != null;
        assert value >= 0 && value < bound;
    }
}
