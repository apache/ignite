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
import static java.util.Collections.emptyList;
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
    public void testEmptyCache() {
        DistributedMetaStorageHistoryCache histCache = new DistributedMetaStorageHistoryCache();

        validate(histCache, emptyList(), 0);

        assertNull(histCache.get(0));
    }

    /** */
    @Test
    public void testPutSingle() {
        DistributedMetaStorageHistoryCache histCache = new DistributedMetaStorageHistoryCache();

        DistributedMetaStorageHistoryItem item0 = newHistoryItem("key0");

        histCache.put(25L, item0);

        validate(histCache, singletonList(item0), -1L);

        assertEquals(item0, histCache.get(25L));

        assertTrue(histCache.sizeInBytes() > 0L);
    }

    /** */
    @Test
    public void testPutWrongVersion() {
        DistributedMetaStorageHistoryCache histCache = new DistributedMetaStorageHistoryCache();

        DistributedMetaStorageHistoryItem item0 = newHistoryItem("key0");

        histCache.put(25L, item0);

        long sizeInBytes = histCache.sizeInBytes();

        // Put with wrong history should throw default assertion error or do nothing.
        try {
            histCache.put(30L, newHistoryItem("key1"));
        }
        catch (Throwable ignore) {
        }

        validate(histCache, singletonList(item0), sizeInBytes);
    }

    /** */
    @Test
    public void testPutMultiple() {
        DistributedMetaStorageHistoryCache histCache = new DistributedMetaStorageHistoryCache();

        DistributedMetaStorageHistoryItem item0 = newHistoryItem("key0");
        DistributedMetaStorageHistoryItem item1 = newHistoryItem("key1");

        histCache.put(25L, item0);

        long sizeInBytes = histCache.sizeInBytes();

        histCache.put(26L, item1);

        validate(histCache, asList(item0, item1), sizeInBytes * 2L);
    }

    /** */
    @Test
    public void testRemove() {
        DistributedMetaStorageHistoryCache histCache = new DistributedMetaStorageHistoryCache();

        DistributedMetaStorageHistoryItem item0 = newHistoryItem("key0");
        DistributedMetaStorageHistoryItem item1 = newHistoryItem("key1");

        histCache.put(25L, item0);

        long sizeInBytes = histCache.sizeInBytes();

        histCache.put(26L, item1);

        histCache.removeOldest();

        validate(histCache, singletonList(item1), sizeInBytes);

        assertNull(histCache.get(25L));

        assertEquals(item1, histCache.get(26L));
    }

    /** */
    @Test
    public void testClear() {
        DistributedMetaStorageHistoryCache histCache = new DistributedMetaStorageHistoryCache();

        DistributedMetaStorageHistoryItem item0 = newHistoryItem("key0");
        DistributedMetaStorageHistoryItem item1 = newHistoryItem("key1");

        histCache.put(25L, item0);
        histCache.put(26L, item1);

        histCache.clear();

        validate(histCache, emptyList(), 0);
    }

    /** */
    @Test
    public void testExpand() {
        DistributedMetaStorageHistoryCache histCache = new DistributedMetaStorageHistoryCache();

        List<DistributedMetaStorageHistoryItem> expect = new LinkedList<>();

        long ver = 0L;

        // Default 16-elements internal array will fit 15 elements without expanding.
        for (; ver < 15L; ver++) {
            DistributedMetaStorageHistoryItem newItem = newHistoryItem(fixedLengthString(ver));

            histCache.put(ver, newItem);

            expect.add(newItem);
        }

        // Size of the single item. They all have the same size in this test.
        long sizeInBytes = histCache.sizeInBytes() / 15L;

        validate(histCache, expect, sizeInBytes * 15L);

        // Clear the beginning of the array.
        for (int i = 0; i < 5; i++) {
            DistributedMetaStorageHistoryItem oldest = histCache.removeOldest();

            DistributedMetaStorageHistoryItem expOldest = expect.remove(0);

            assertEquals(expOldest, oldest);
        }

        validate(histCache, expect, sizeInBytes * 10L);

        // Overlap data over the end of the internal array.
        for (; ver < 20L; ver++) {
            DistributedMetaStorageHistoryItem newItem = newHistoryItem(fixedLengthString(ver));

            histCache.put(ver, newItem);

            expect.add(newItem);
        }

        validate(histCache, expect, sizeInBytes * 15L);

        // Expand the internal array.
        for (; ver < 25L; ver++) {
            DistributedMetaStorageHistoryItem newItem = newHistoryItem(fixedLengthString(ver));

            histCache.put(ver, newItem);

            expect.add(newItem);
        }

        validate(histCache, expect, sizeInBytes * 20L);
    }

    /** */
    private static DistributedMetaStorageHistoryItem newHistoryItem(String key) {
        return new DistributedMetaStorageHistoryItem(key, EMPTY_BYTE_ARRAY);
    }

    /** */
    private static void validate(
        DistributedMetaStorageHistoryCache histCache,
        List<DistributedMetaStorageHistoryItem> exp,
        long expSizeInBytes
    ) {
        if (exp.isEmpty())
            assertTrue(histCache.isEmpty());
        else
            assertFalse(histCache.isEmpty());

        assertEquals(exp.size(), histCache.size());

        if (expSizeInBytes >= 0L)
            assertEquals(expSizeInBytes, histCache.sizeInBytes());

        assertEquals(exp, asList(histCache.toArray()));
    }

    /** */
    private static String fixedLengthString(long ver) {
        assertTrue(ver < 100);

        return Long.toString(100 + ver);
    }
}
