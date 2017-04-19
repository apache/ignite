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

package org.apache.ignite.internal.processors.query.h2.database;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.UUID;
import junit.framework.TestCase;
import org.apache.commons.io.Charsets;
import org.apache.ignite.internal.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.impl.PageMemoryNoStoreImpl;
import org.apache.ignite.internal.processors.cache.database.MemoryMetricsImpl;
import org.apache.ignite.logger.java.JavaLogger;
import org.h2.result.SortOrder;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueByte;
import org.h2.value.ValueBytes;
import org.h2.value.ValueDate;
import org.h2.value.ValueDouble;
import org.h2.value.ValueFloat;
import org.h2.value.ValueInt;
import org.h2.value.ValueLong;
import org.h2.value.ValueNull;
import org.h2.value.ValueShort;
import org.h2.value.ValueString;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueUuid;

/**
 * Simple tests for {@link InlineIndexHelper}.
 */
public class InlineIndexHelperTest extends TestCase {
    /** */
    private static final int CACHE_ID = 42;

    /** */
    private static final int PAGE_SIZE = 1024;

    /** */
    private static final long MB = 1024;

    /** */
    private static final int CPUS = Runtime.getRuntime().availableProcessors();

    /** Test utf-8 string cutting. */
    public void testConvert() {
        // 8 bytes total: 1b, 1b, 3b, 3b.

        byte[] bytes = InlineIndexHelper.trimUTF8("00\u20ac\u20ac".getBytes(Charsets.UTF_8), 7);
        assertEquals(5, bytes.length);

        String s = new String(bytes);
        assertEquals(3, s.length());

        bytes = InlineIndexHelper.trimUTF8("aaaaaa".getBytes(Charsets.UTF_8), 4);
        assertEquals(4, bytes.length);
    }

    /** Limit is too small to cut */
    public void testStringCut() {
        // 6 bytes total: 3b, 3b.

        byte[] bytes = InlineIndexHelper.trimUTF8("\u20ac\u20ac".getBytes(Charsets.UTF_8), 2);
        assertNull(bytes);
    }

    /** Test on String values compare */
    public void testRelyOnCompare() {
        InlineIndexHelper ha = new InlineIndexHelper(Value.STRING, 0, SortOrder.ASCENDING);

        // same size
        assertFalse(getRes(ha, "aabb", "aabb"));

        // second string is shorter
        assertTrue(getRes(ha, "aabb", "aac"));
        assertTrue(getRes(ha, "aabb", "aaa"));

        // second string is longer
        assertTrue(getRes(ha, "aabb", "aaaaaa"));
        assertFalse(getRes(ha, "aaa", "aaaaaa"));

        // one is null
        assertTrue(getRes(ha, "a", null));
        assertTrue(getRes(ha, null, "a"));
        assertTrue(getRes(ha, null, null));
    }

    /** Test on Bytes values compare */
    public void testRelyOnCompareBytes() {
        InlineIndexHelper ha = new InlineIndexHelper(Value.BYTES, 0, SortOrder.ASCENDING);

        // same size
        assertFalse(getResBytes(ha, new byte[] {1, 2, 3, 4}, new byte[] {1, 2, 3, 4}));

        // second aray is shorter
        assertTrue(getResBytes(ha, new byte[] {1, 2, 2, 2}, new byte[] {1, 1, 2}));
        assertTrue(getResBytes(ha, new byte[] {1, 1, 1, 2}, new byte[] {1, 1, 2}));

        // second array is longer
        assertTrue(getResBytes(ha, new byte[] {1, 2}, new byte[] {1, 1, 1}));
        assertFalse(getResBytes(ha, new byte[] {1, 1}, new byte[] {1, 1, 2}));

        // one is null
        assertTrue(getResBytes(ha, new byte[] {1, 2, 3, 4}, null));
        assertTrue(getResBytes(ha, null, new byte[] {1, 2, 3, 4}));
        assertTrue(getResBytes(ha, null, null));
    }

    /** */
    public void testStringTruncate() throws Exception {
        long[] sizes = new long[CPUS];

        for (int i = 0; i < sizes.length; i++)
            sizes[i] = 1024 * MB / CPUS;

        PageMemory pageMem = new PageMemoryNoStoreImpl(new JavaLogger(),
            new UnsafeMemoryProvider(sizes),
            null,
            PAGE_SIZE,
            new MemoryMetricsImpl(null),
            false);

        pageMem.start();

        long pageId = 0L;
        long page = 0L;

        try {
            pageId = pageMem.allocatePage(CACHE_ID, 1, PageIdAllocator.FLAG_DATA);
            page = pageMem.acquirePage(CACHE_ID, pageId);
            long pageAddr = pageMem.readLock(CACHE_ID, pageId, page);

            int off = 0;

            InlineIndexHelper ih = new InlineIndexHelper(Value.STRING, 1, 0);
            ih.put(pageAddr, off, ValueString.get("aaaaaaa"), 3 + 5);

            assertFalse(ih.isValueFull(pageAddr, off));

            assertEquals("aaaaa", ih.get(pageAddr, off, 3 + 5).getString());

            ih.put(pageAddr, off, ValueString.get("aaa"), 3 + 5);

            assertTrue(ih.isValueFull(pageAddr, off));

            assertEquals("aaa", ih.get(pageAddr, off, 3 + 5).getString());
        }
        finally {
            if (page != 0L)
                pageMem.releasePage(CACHE_ID, pageId, page);
            pageMem.stop();
        }
    }

    /** */
    public void testBytes() throws Exception {
        long[] sizes = new long[CPUS];

        for (int i = 0; i < sizes.length; i++)
            sizes[i] = 1024 * MB / CPUS;

        PageMemory pageMem = new PageMemoryNoStoreImpl(new JavaLogger(),
            new UnsafeMemoryProvider(sizes),
            null,
            PAGE_SIZE,
            new MemoryMetricsImpl(null),
            false);

        pageMem.start();

        long pageId = 0L;
        long page = 0L;

        try {
            pageId = pageMem.allocatePage(CACHE_ID, 1, PageIdAllocator.FLAG_DATA);
            page = pageMem.acquirePage(CACHE_ID, pageId);
            long pageAddr = pageMem.readLock(CACHE_ID, pageId, page);

            int off = 0;

            InlineIndexHelper ih = new InlineIndexHelper(Value.BYTES, 1, 0);

            ih.put(pageAddr, off, ValueBytes.get(new byte[] {1, 2, 3, 4, 5}), 3 + 3);

            assertFalse(ih.isValueFull(pageAddr, off));

            assertTrue(Arrays.equals(new byte[] {1, 2, 3}, ih.get(pageAddr, off, 3 + 5).getBytes()));

            ih.put(pageAddr, off, ValueBytes.get(new byte[] {1, 2, 3, 4, 5}), 3 + 5);

            assertTrue(ih.isValueFull(pageAddr, off));

            assertTrue(Arrays.equals(new byte[] {1, 2, 3, 4, 5}, ih.get(pageAddr, off, 3 + 5).getBytes()));
        }
        finally {
            if (page != 0L)
                pageMem.releasePage(CACHE_ID, pageId, page);
            pageMem.stop();
        }
    }

    /** */
    public void testNull() throws Exception {
        testPutGet(ValueInt.get(-1), ValueNull.INSTANCE, ValueInt.get(3));
    }

    /** */
    public void testBoolean() throws Exception {
        testPutGet(ValueBoolean.get(true), ValueBoolean.get(false), ValueBoolean.get(true));
    }

    /** */
    public void testByte() throws Exception {
        testPutGet(ValueByte.get((byte)-1), ValueByte.get((byte)2), ValueByte.get((byte)3));
    }

    /** */
    public void testShort() throws Exception {
        testPutGet(ValueShort.get((short)-32000), ValueShort.get((short)2), ValueShort.get((short)3));
    }

    /** */
    public void testInt() throws Exception {
        testPutGet(ValueInt.get(-1), ValueInt.get(2), ValueInt.get(3));
    }

    /** */
    public void testLong() throws Exception {
        testPutGet(ValueLong.get(-1), ValueLong.get(2), ValueLong.get(3));
    }

    /** */
    public void testFloat() throws Exception {
        testPutGet(ValueFloat.get(1.1f), ValueFloat.get(2.2f), ValueFloat.get(1.1f));
    }

    /** */
    public void testDouble() throws Exception {
        testPutGet(ValueDouble.get(1.1f), ValueDouble.get(2.2f), ValueDouble.get(1.1f));
    }

    /** */
    public void testDate() throws Exception {
        testPutGet(ValueDate.get(Date.valueOf("2017-02-20")),
            ValueDate.get(Date.valueOf("2017-02-21")),
            ValueDate.get(Date.valueOf("2017-02-19")));
    }

    /** */
    public void testTime() throws Exception {
        testPutGet(ValueTime.get(Time.valueOf("10:01:01")),
            ValueTime.get(Time.valueOf("11:02:02")),
            ValueTime.get(Time.valueOf("12:03:03")));
    }

    /** */
    public void testTimestamp() throws Exception {
        testPutGet(ValueTimestamp.get(Timestamp.valueOf("2017-02-20 10:01:01")),
            ValueTimestamp.get(Timestamp.valueOf("2017-02-20 10:01:01")),
            ValueTimestamp.get(Timestamp.valueOf("2017-02-20 10:01:01")));
    }

    /** */
    public void testUUID() throws Exception {
        testPutGet(ValueUuid.get(UUID.randomUUID().toString()),
            ValueUuid.get(UUID.randomUUID().toString()),
            ValueUuid.get(UUID.randomUUID().toString()));
    }

    /** */
    private void testPutGet(Value v1, Value v2, Value v3) throws Exception {
        long[] sizes = new long[CPUS];

        for (int i = 0; i < sizes.length; i++)
            sizes[i] = 1024 * MB / CPUS;

        PageMemory pageMem = new PageMemoryNoStoreImpl(new JavaLogger(),
            new UnsafeMemoryProvider(sizes),
            null,
            PAGE_SIZE,
            new MemoryMetricsImpl(null),
            false);

        pageMem.start();

        long pageId = 0L;
        long page = 0L;

        try {
            pageId = pageMem.allocatePage(CACHE_ID, 1, PageIdAllocator.FLAG_DATA);
            page = pageMem.acquirePage(CACHE_ID, pageId);
            long pageAddr = pageMem.readLock(CACHE_ID, pageId, page);

            int off = 0;
            int max = 255;

            InlineIndexHelper ih = new InlineIndexHelper(v1.getType(), 1, 0);

            off += ih.put(pageAddr, off, v1, max - off);
            off += ih.put(pageAddr, off, v2, max - off);
            off += ih.put(pageAddr, off, v3, max - off);

            Value v11 = ih.get(pageAddr, 0, max);
            Value v22 = ih.get(pageAddr, ih.fullSize(pageAddr, 0), max);

            assertEquals(v1.getObject(), v11.getObject());
            assertEquals(v2.getObject(), v22.getObject());
        }
        finally {
            if (page != 0L)
                pageMem.releasePage(CACHE_ID, pageId, page);
            pageMem.stop();
        }
    }

    /** */
    private boolean getRes(InlineIndexHelper ha, String s1, String s2) {
        Value v1 = s1 == null ? ValueNull.INSTANCE : ValueString.get(s1);
        Value v2 = s2 == null ? ValueNull.INSTANCE : ValueString.get(s2);

        int c = v1.compareTypeSafe(v2, CompareMode.getInstance(CompareMode.DEFAULT, 0));
        return ha.canRelyOnCompare(c, v1, v2);
    }

    /** */
    private boolean getResBytes(InlineIndexHelper ha, byte[] b1, byte[] b2) {
        Value v1 = b1 == null ? ValueNull.INSTANCE : ValueBytes.get(b1);
        Value v2 = b2 == null ? ValueNull.INSTANCE : ValueBytes.get(b2);

        int c = v1.compareTypeSafe(v2, CompareMode.getInstance(CompareMode.DEFAULT, 0));
        return ha.canRelyOnCompare(c, v1, v2);
    }

}