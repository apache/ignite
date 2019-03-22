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

package org.apache.ignite.internal.processors.metastorage.persistence;

import java.util.LinkedList;
import java.util.List;
import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** */
public class DistributedMetaStorageHistoryCacheTest {
    /** */
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    /** */
    @Test
    public void testBasicOperations() {
        // Empty cache.
        DistributedMetaStorageHistoryCache histCache = new DistributedMetaStorageHistoryCache();

        assertTrue(histCache.isEmpty());

        assertEquals(0, histCache.size());

        assertEquals(0L, histCache.sizeInBytes());

        assertEquals(0, histCache.toArray().length);

        assertNull(histCache.get(0));

        // Cache with one element.
        DistributedMetaStorageHistoryItem item0 = newHistoryItem("key0");

        histCache.put(25L, item0);

        assertFalse(histCache.isEmpty());

        assertEquals(1, histCache.size());

        long sizeInBytes = histCache.sizeInBytes();

        assertEquals(item0, histCache.get(25L));

        assertEquals(singletonList(item0), asList(histCache.toArray()));

        DistributedMetaStorageHistoryItem item1 = newHistoryItem("key1");

        // Put with wrong history should throw default assertion error or do nothing.
        try {
            histCache.put(30L, item1);
        }
        catch (Throwable ignore) {
        }

        assertEquals(1, histCache.size());

        assertEquals(sizeInBytes, histCache.sizeInBytes());

        // Cache with two elements.
        histCache.put(26L, item1);

        assertEquals(2, histCache.size());

        assertEquals(sizeInBytes * 2, histCache.sizeInBytes());

        assertEquals(asList(item0, item1), asList(histCache.toArray()));

        // Remove oldest element, one left.
        histCache.removeOldest();

        assertEquals(1, histCache.size());

        assertEquals(sizeInBytes, histCache.sizeInBytes());

        assertNull(histCache.get(25L));

        assertEquals(item1, histCache.get(26L));

        assertEquals(singletonList(item1), asList(histCache.toArray()));

        // Empty cache again.
        histCache.clear();

        assertTrue(histCache.isEmpty());

        assertEquals(0, histCache.size());

        assertEquals(0L, histCache.sizeInBytes());

        assertEquals(0, histCache.toArray().length);
    }

    /** */
    @Test
    public void testExpand() {
        DistributedMetaStorageHistoryCache histCache = new DistributedMetaStorageHistoryCache();

        List<DistributedMetaStorageHistoryItem> expect = new LinkedList<>();

        long ver = 0L;

        // Default 16-elemets internal array will fit 15 elemet without expanding.
        for (; ver < 15L; ver++) {
            DistributedMetaStorageHistoryItem newItem = newHistoryItem(Long.toString(ver));

            histCache.put(ver, newItem);

            expect.add(newItem);
        }

        assertEquals(expect, asList(histCache.toArray()));

        // Clear the beginning of the array.
        for (int i = 0; i < 5; i++) {
            DistributedMetaStorageHistoryItem oldest = histCache.removeOldest();

            DistributedMetaStorageHistoryItem expOldest = expect.remove(0);

            assertEquals(expOldest, oldest);
        }

        assertEquals(expect, asList(histCache.toArray()));

        // Overlap data through the end of the internal array.
        for (; ver < 20L; ver++) {
            DistributedMetaStorageHistoryItem newItem = newHistoryItem(Long.toString(ver));

            histCache.put(ver, newItem);

            expect.add(newItem);
        }

        assertEquals(expect, asList(histCache.toArray()));

        // Expand the internal array.
        for (; ver < 25L; ver++) {
            DistributedMetaStorageHistoryItem newItem = newHistoryItem(Long.toString(ver));

            histCache.put(ver, newItem);

            expect.add(newItem);
        }

        assertEquals(expect, asList(histCache.toArray()));
    }

    /** */
    private static DistributedMetaStorageHistoryItem newHistoryItem(String key) {
        return new DistributedMetaStorageHistoryItem(key, EMPTY_BYTE_ARRAY);
    }
}
