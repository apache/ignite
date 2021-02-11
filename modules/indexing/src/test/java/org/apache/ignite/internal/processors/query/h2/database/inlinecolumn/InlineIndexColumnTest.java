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

package org.apache.ignite.internal.processors.query.h2.database.inlinecolumn;

import java.io.Serializable;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Comparator;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.io.Charsets;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.internal.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.impl.PageMemoryNoStoreImpl;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.processors.query.h2.database.InlineIndexColumn;
import org.apache.ignite.testframework.junits.GridTestBinaryMarshaller;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.h2.table.Column;
import org.h2.util.DateTimeUtils;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueByte;
import org.h2.value.ValueBytes;
import org.h2.value.ValueDate;
import org.h2.value.ValueDouble;
import org.h2.value.ValueFloat;
import org.h2.value.ValueInt;
import org.h2.value.ValueJavaObject;
import org.h2.value.ValueLong;
import org.h2.value.ValueNull;
import org.h2.value.ValueShort;
import org.h2.value.ValueString;
import org.h2.value.ValueStringFixed;
import org.h2.value.ValueStringIgnoreCase;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueUuid;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.h2.database.inlinecolumn.AbstractInlineIndexColumn.CANT_BE_COMPARE;

/**
 * Simple tests for {@link InlineIndexColumn}.
 */
@WithSystemProperty(key = "h2.serializeJavaObject", value = "false")
@WithSystemProperty(key = "h2.objectCache", value = "false")
public class InlineIndexColumnTest extends AbstractIndexingCommonTest {
    /** */
    private static final int CACHE_ID = 42;

    /** */
    private static final int PAGE_SIZE = 1024;

    /** */
    private static final long MB = 1024;

    /** */
    private static final Comparator<Value> ALWAYS_FAILS_COMPARATOR = new AlwaysFailsComparator();

    /** */
    private final GridTestBinaryMarshaller marsh = new GridTestBinaryMarshaller(log);

    /** Whether to inline objects as hash or as bytes. */
    private boolean inlineObjHash;

    /** */
    @Before
    public void resetState() {
        inlineObjHash = false;
    }

    /** Test utf-8 string cutting. */
    @Test
    public void testConvert() {
        // 8 bytes total: 1b, 1b, 3b, 3b.
        byte[] bytes = StringInlineIndexColumn.trimUTF8("00\u20ac\u20ac".getBytes(Charsets.UTF_8), 7);
        assertEquals(5, bytes.length);

        String s = new String(bytes);
        assertEquals(3, s.length());

        bytes = StringInlineIndexColumn.trimUTF8("aaaaaa".getBytes(Charsets.UTF_8), 4);
        assertEquals(4, bytes.length);
    }

    /** */
    @Test
    public void testCompare1bytes() throws Exception {
        int maxSize = 3 + 2; // 2 ascii chars + 3 bytes header.

        assertEquals(0, putAndCompare("aa", "aa", String.class, maxSize));
        assertEquals(-1, putAndCompare("aa", "bb", String.class, maxSize));
        assertEquals(-1, putAndCompare("aaa", "bbb", String.class, maxSize));
        assertEquals(1, putAndCompare("bbb", "aaa", String.class, maxSize));
        assertEquals(1, putAndCompare("aaa", "aa", String.class, maxSize));
        assertEquals(1, putAndCompare("aaa", "a", String.class, maxSize));
        assertEquals(CANT_BE_COMPARE, putAndCompare("aaa", "aaa", String.class, maxSize));
        assertEquals(CANT_BE_COMPARE, putAndCompare("aaa", "aab", String.class, maxSize));
        assertEquals(CANT_BE_COMPARE, putAndCompare("aab", "aaa", String.class, maxSize));
    }

    /** */
    @Test
    public void testCompare2bytes() throws Exception {
        int maxSize = 3 + 4; // 2 2-bytes chars + 3 bytes header.

        assertEquals(0, putAndCompare("¡¡", "¡¡", String.class, maxSize));
        assertEquals(-1, putAndCompare("¡¡", "¢¢", String.class, maxSize));
        assertEquals(-1, putAndCompare("¡¡¡", "¢¢¢", String.class, maxSize));
        assertEquals(1, putAndCompare("¢¢¢", "¡¡¡", String.class, maxSize));
        assertEquals(1, putAndCompare("¡¡¡", "¡¡", String.class, maxSize));
        assertEquals(1, putAndCompare("¡¡¡", "¡", String.class, maxSize));
        assertEquals(CANT_BE_COMPARE, putAndCompare("¡¡¡", "¡¡¡", String.class, maxSize));
        assertEquals(CANT_BE_COMPARE, putAndCompare("¡¡¡", "¡¡¢", String.class, maxSize));
        assertEquals(CANT_BE_COMPARE, putAndCompare("¡¡¢", "¡¡¡", String.class, maxSize));
    }

    /** */
    @Test
    public void testCompare3bytes() throws Exception {
        int maxSize = 3 + 6; // 2 3-bytes chars + 3 bytes header.

        assertEquals(0, putAndCompare("ऄऄ", "ऄऄ", String.class, maxSize));
        assertEquals(-1, putAndCompare("ऄऄ", "अअ", String.class, maxSize));
        assertEquals(-1, putAndCompare("ऄऄऄ", "अअअ", String.class, maxSize));
        assertEquals(1, putAndCompare("अअअ", "ऄऄऄ", String.class, maxSize));
        assertEquals(1, putAndCompare("ऄऄऄ", "ऄऄ", String.class, maxSize));
        assertEquals(1, putAndCompare("ऄऄऄ", "ऄ", String.class, maxSize));
        assertEquals(-2, putAndCompare("ऄऄऄ", "ऄऄऄ", String.class, maxSize));
        assertEquals(-2, putAndCompare("ऄऄऄ", "ऄऄअ", String.class, maxSize));
        assertEquals(-2, putAndCompare("ऄऄअ", "ऄऄऄ", String.class, maxSize));
    }

    /** */
    @Test
    public void testCompare4bytes() throws Exception {
        int maxSize = 3 + 8; // 2 4-bytes chars + 3 bytes header.

        assertEquals(0, putAndCompare("\ud802\udd20\ud802\udd20", "\ud802\udd20\ud802\udd20", String.class, maxSize));
        assertEquals(-1, putAndCompare("\ud802\udd20\ud802\udd20", "\ud802\udd21\ud802\udd21", String.class, maxSize));
        assertEquals(-1, putAndCompare("\ud802\udd20\ud802\udd20\ud802\udd20", "\ud802\udd21\ud802\udd21\ud802\udd21", String.class, maxSize));
        assertEquals(1, putAndCompare("\ud802\udd21\ud802\udd21\ud802\udd21", "\ud802\udd20\ud802\udd20\ud802\udd20", String.class, maxSize));
        assertEquals(1, putAndCompare("\ud802\udd20\ud802\udd20\ud802\udd20", "\ud802\udd20\ud802\udd20", String.class, maxSize));
        assertEquals(1, putAndCompare("\ud802\udd20\ud802\udd20\ud802\udd20", "\ud802\udd20", String.class, maxSize));
        assertEquals(-2, putAndCompare("\ud802\udd20\ud802\udd20\ud802\udd20", "\ud802\udd20\ud802\udd20\ud802\udd20", String.class, maxSize));
        assertEquals(-2, putAndCompare("\ud802\udd20\ud802\udd20\ud802\udd20", "\ud802\udd20\ud802\udd20\ud802\udd21", String.class, maxSize));
        assertEquals(-2, putAndCompare("\ud802\udd20\ud802\udd20\ud802\udd21", "\ud802\udd20\ud802\udd20\ud802\udd20", String.class, maxSize));
    }

    /** */
    @Test
    public void testCompareMixed() throws Exception {
        int maxSize = 3 + 8; // 2 up to 4-bytes chars + 3 bytes header.

        assertEquals(0, putAndCompare("\ud802\udd20\u0904", "\ud802\udd20\u0904", String.class, maxSize));
        assertEquals(-1, putAndCompare("\ud802\udd20\u0904", "\ud802\udd20\u0905", String.class, maxSize));
        assertEquals(1, putAndCompare("\u0905\ud802\udd20", "\u0904\ud802\udd20", String.class, maxSize));
        assertEquals(-2, putAndCompare("\ud802\udd20\ud802\udd20\u0905", "\ud802\udd20\ud802\udd20\u0904", String.class, maxSize));
    }

    /** */
    @Test
    public void testCompareMixed2() throws Exception {
        int strCnt = 1000;
        int symbCnt = 20;
        int inlineSize = symbCnt * 4 + 3;

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        String[] strings = new String[strCnt];

        for (int i = 0; i < strings.length; i++)
            strings[i] = randomString(symbCnt);

        Arrays.sort(strings);

        for (int i = 0; i < 100; i++) {
            int i1 = rnd.nextInt(strings.length);
            int i2 = rnd.nextInt(strings.length);

            assertEquals(Integer.compare(i1, i2), putAndCompare(strings[i1], strings[i2], String.class, inlineSize));
        }
    }

    /**
     * @param v1 Value 1.
     * @param v2 Value 2.
     * @param maxSize Max inline size.
     * @return Compare result.
     * @throws Exception If failed.
     */
    private <T> int putAndCompare(T v1, T v2, Class<T> cls, int maxSize) throws Exception {
        DataRegionConfiguration plcCfg = new DataRegionConfiguration().setInitialSize(1024 * MB)
            .setMaxSize(1024 * MB);

        PageMemory pageMem = new PageMemoryNoStoreImpl(log,
            new UnsafeMemoryProvider(log),
            null,
            PAGE_SIZE,
            plcCfg,
            new LongAdderMetric("NO_OP", null),
            false);

        pageMem.start();

        long pageId = 0L;
        long page = 0L;

        try {
            pageId = pageMem.allocatePage(CACHE_ID, 1, PageIdAllocator.FLAG_DATA);
            page = pageMem.acquirePage(CACHE_ID, pageId);
            long pageAddr = pageMem.readLock(CACHE_ID, pageId, page);

            int off = 0;

            InlineIndexColumnFactory factory = new InlineIndexColumnFactory(CompareMode.getInstance(CompareMode.OFF, 1));

            InlineIndexColumn ih = factory.createInlineHelper(new Column("", wrap(v1, cls).getType()), inlineObjHash);

            ih.put(pageAddr, off, wrap(v1, cls), maxSize);

            return ih.compare(pageAddr, off, maxSize, wrap(v2, cls), ALWAYS_FAILS_COMPARATOR);
        }
        finally {
            if (page != 0L)
                pageMem.releasePage(CACHE_ID, pageId, page);

            pageMem.stop(true);
        }
    }

    /** Limit is too small to cut */
    @Test
    public void testStringCut() {
        // 6 bytes total: 3b, 3b.
        byte[] bytes = StringInlineIndexColumn.trimUTF8("\u20ac\u20ac".getBytes(Charsets.UTF_8), 2);

        assertNull(bytes);
    }


    /** */
    @Test
    public void testStringTruncate() throws Exception {
        DataRegionConfiguration plcCfg = new DataRegionConfiguration().setInitialSize(1024 * MB)
            .setMaxSize(1024 * MB);

        PageMemory pageMem = new PageMemoryNoStoreImpl(log(),
            new UnsafeMemoryProvider(log()),
            null,
            PAGE_SIZE,
            plcCfg,
            new LongAdderMetric("NO_OP", null),
            false);

        pageMem.start();

        long pageId = 0L;
        long page = 0L;

        try {
            pageId = pageMem.allocatePage(CACHE_ID, 1, PageIdAllocator.FLAG_DATA);
            page = pageMem.acquirePage(CACHE_ID, pageId);
            long pageAddr = pageMem.readLock(CACHE_ID, pageId, page);

            int off = 0;

            InlineIndexColumnFactory factory = new InlineIndexColumnFactory(CompareMode.getInstance(CompareMode.DEFAULT, 1));

            AbstractInlineIndexColumn ih = (AbstractInlineIndexColumn)factory.createInlineHelper(new Column("", Value.STRING), false);

            ih.put(pageAddr, off, ValueString.get("aaaaaaa"), 3 + 5);

            assertEquals("aaaaa", ih.get(pageAddr, off, 3 + 5).getString());

            ih.put(pageAddr, off, ValueString.get("aaa"), 3 + 5);

            assertEquals("aaa", ih.get(pageAddr, off, 3 + 5).getString());

            ih.put(pageAddr, off, ValueString.get("\u20acaaa"), 3 + 2);

            assertNull(ih.get(pageAddr, off, 3 + 2));
        }
        finally {
            if (page != 0L)
                pageMem.releasePage(CACHE_ID, pageId, page);
            pageMem.stop(true);
        }
    }

    /** */
    @Test
    public void testBytes() throws Exception {
        DataRegionConfiguration plcCfg = new DataRegionConfiguration().setInitialSize(1024 * MB)
            .setMaxSize(1024 * MB);

        PageMemory pageMem = new PageMemoryNoStoreImpl(log(),
            new UnsafeMemoryProvider(log()),
            null,
            PAGE_SIZE,
            plcCfg,
            new LongAdderMetric("NO_OP", null),
            false);

        pageMem.start();

        long pageId = 0L;
        long page = 0L;

        try {
            pageId = pageMem.allocatePage(CACHE_ID, 1, PageIdAllocator.FLAG_DATA);
            page = pageMem.acquirePage(CACHE_ID, pageId);
            long pageAddr = pageMem.readLock(CACHE_ID, pageId, page);

            int off = 0;

            InlineIndexColumnFactory factory = new InlineIndexColumnFactory(CompareMode.getInstance(CompareMode.DEFAULT, 1));

            AbstractInlineIndexColumn ih = (AbstractInlineIndexColumn)factory.createInlineHelper(new Column("", Value.BYTES), false);

            int maxSize = 3 + 3;
            int savedBytesCnt = ih.put(pageAddr, off, ValueBytes.get(new byte[] {1, 2, 3, 4, 5}), maxSize);

            assertTrue(savedBytesCnt > 0);

            assertTrue(savedBytesCnt <= maxSize);

            maxSize = 3 + 5;

            assertTrue(Arrays.equals(new byte[] {1, 2, 3}, ih.get(pageAddr, off, maxSize).getBytes()));

            savedBytesCnt = ih.put(pageAddr, off, ValueBytes.get(new byte[] {1, 2, 3, 4, 5}), maxSize);

            assertTrue(savedBytesCnt > 0);

            assertTrue(savedBytesCnt <= maxSize);

            assertTrue(Arrays.equals(new byte[] {1, 2, 3, 4, 5}, ih.get(pageAddr, off, maxSize).getBytes()));
        }
        finally {
            if (page != 0L)
                pageMem.releasePage(CACHE_ID, pageId, page);
            pageMem.stop(true);
        }
    }

    /** */
    @Test
    public void testJavaObjectInlineBytes() throws Exception {
        DataRegionConfiguration plcCfg = new DataRegionConfiguration().setInitialSize(1024 * MB)
            .setMaxSize(1024 * MB);

        PageMemory pageMem = new PageMemoryNoStoreImpl(log(),
            new UnsafeMemoryProvider(log()),
            null,
            PAGE_SIZE,
            plcCfg,
            new LongAdderMetric("NO_OP", null),
            false);

        pageMem.start();

        long pageId = 0L;
        long page = 0L;

        try {
            pageId = pageMem.allocatePage(CACHE_ID, 1, PageIdAllocator.FLAG_DATA);
            page = pageMem.acquirePage(CACHE_ID, pageId);
            long pageAddr = pageMem.readLock(CACHE_ID, pageId, page);

            int off = 0;

            InlineIndexColumnFactory factory = new InlineIndexColumnFactory(CompareMode.getInstance(CompareMode.DEFAULT, 1));

            AbstractInlineIndexColumn ih = (AbstractInlineIndexColumn)factory.createInlineHelper(new Column("", Value.JAVA_OBJECT), false);

            ValueJavaObject exp = ValueJavaObject.getNoCopy(new TestPojo(4, 3L), null, null);

            int maxSize = 3 + 3;
            int savedBytesCnt = ih.put(pageAddr, off, exp, maxSize);

            assertTrue(savedBytesCnt > 0);

            assertTrue(savedBytesCnt <= maxSize);

            maxSize = 3 + exp.getBytesNoCopy().length;

            assertTrue(Arrays.equals(Arrays.copyOf(exp.getBytesNoCopy(), 3), ih.get(pageAddr, off, maxSize).getBytes()));

            savedBytesCnt = ih.put(pageAddr, off, ValueJavaObject.getNoCopy(null, exp.getBytesNoCopy(), null), maxSize);

            assertTrue(savedBytesCnt > 0);

            assertTrue(savedBytesCnt <= maxSize);

            assertTrue(Arrays.equals(exp.getBytesNoCopy(), ih.get(pageAddr, off, maxSize).getBytes()));
        }
        finally {
            if (page != 0L)
                pageMem.releasePage(CACHE_ID, pageId, page);
            pageMem.stop(true);
        }
    }

    /** */
    @Test
    public void testJavaObjectInlineHash() throws Exception {
        DataRegionConfiguration plcCfg = new DataRegionConfiguration().setInitialSize(1024 * MB)
            .setMaxSize(1024 * MB);

        PageMemory pageMem = new PageMemoryNoStoreImpl(log(),
            new UnsafeMemoryProvider(log()),
            null,
            PAGE_SIZE,
            plcCfg,
            new LongAdderMetric("NO_OP", null),
            false);

        pageMem.start();

        long pageId = 0L;
        long page = 0L;

        try {
            pageId = pageMem.allocatePage(CACHE_ID, 1, PageIdAllocator.FLAG_DATA);
            page = pageMem.acquirePage(CACHE_ID, pageId);
            long pageAddr = pageMem.readLock(CACHE_ID, pageId, page);

            int off = 0;

            InlineIndexColumnFactory factory = new InlineIndexColumnFactory(CompareMode.getInstance(CompareMode.DEFAULT, 1));

            InlineIndexColumn ih = factory.createInlineHelper(new Column("", Value.JAVA_OBJECT), true);

            Value exp = wrap(new TestPojo(4, 3L), TestPojo.class);

            {
                int maxSize = 3;

                int savedBytesCnt = ih.put(pageAddr, off, exp, maxSize);

                Assert.assertEquals(0, savedBytesCnt);
            }

            {
                int maxSize = 7;

                int savedBytesCnt = ih.put(pageAddr, off, exp, maxSize);

                Assert.assertEquals(5, savedBytesCnt);

                Assert.assertEquals(exp.getObject().hashCode(),
                    ((ObjectHashInlineIndexColumn)ih).inlinedValue(pageAddr, off).getInt());

                Assert.assertEquals(CANT_BE_COMPARE,
                    ih.compare(pageAddr, off, maxSize, exp, null));
            }
        }
        finally {
            if (page != 0L)
                pageMem.releasePage(CACHE_ID, pageId, page);
            pageMem.stop(true);
        }
    }

    /** */
    @Test
    public void testNull() throws Exception {
        testPutGet(ValueInt.get(-1), ValueNull.INSTANCE, ValueInt.get(3));
        testPutGet(ValueInt.get(-1), ValueNull.INSTANCE, ValueInt.get(3));

        int maxSize = 3 + 2; // 2 ascii chars + 3 bytes header.
        assertEquals(1, putAndCompare("aa", null, String.class, maxSize));
    }

    /** */
    @Test
    public void testBoolean() throws Exception {
        testPutGet(ValueBoolean.get(true), ValueBoolean.get(false), ValueBoolean.get(true));

        int maxSize = 1 + 1; // 1 byte header + 1 byte value.

        assertEquals(1, putAndCompare(true, null, Boolean.class, maxSize));
        assertEquals(1, putAndCompare(true, false, Boolean.class, maxSize));
        assertEquals(-1, putAndCompare(false, true, Boolean.class, maxSize));
        assertEquals(0, putAndCompare(false, false, Boolean.class, maxSize));
        assertEquals(0, putAndCompare(true, true, Boolean.class, maxSize));
        assertEquals(-2, putAndCompare(true, false, Boolean.class, maxSize - 1));
    }

    /** */
    @Test
    public void testByte() throws Exception {
        testPutGet(ValueByte.get((byte)-1), ValueByte.get((byte)2), ValueByte.get((byte)3));

        int maxSize = 1 + 1; // 1 byte header + 1 byte value.

        assertEquals(1, putAndCompare((byte)42, null, Byte.class, maxSize));
        assertEquals(1, putAndCompare((byte)42, (byte)16, Byte.class, maxSize));
        assertEquals(-1, putAndCompare((byte)16, (byte)42, Byte.class, maxSize));
        assertEquals(0, putAndCompare((byte)42, (byte)42, Byte.class, maxSize));
        assertEquals(0, putAndCompare(Byte.MAX_VALUE, Byte.MAX_VALUE, Byte.class, maxSize));
        assertEquals(1, putAndCompare(Byte.MAX_VALUE, Byte.MIN_VALUE, Byte.class, maxSize));
        assertEquals(-1, putAndCompare(Byte.MIN_VALUE, Byte.MAX_VALUE, Byte.class, maxSize));
        assertEquals(-2, putAndCompare((byte)42, (byte)16, Byte.class, maxSize - 1));
    }

    /** */
    @Test
    public void testShort() throws Exception {
        testPutGet(ValueShort.get((short)-32000), ValueShort.get((short)2), ValueShort.get((short)3));

        int maxSize = 1 + 2; // 1 byte header + 2 bytes value.

        assertEquals(1, putAndCompare((short)42, null, Short.class, maxSize));
        assertEquals(1, putAndCompare((short)42, (short)16, Short.class, maxSize));
        assertEquals(-1, putAndCompare((short)16, (short)42, Short.class, maxSize));
        assertEquals(0, putAndCompare((short)42, (short)42, Short.class, maxSize));
        assertEquals(0, putAndCompare(Short.MAX_VALUE, Short.MAX_VALUE, Short.class, maxSize));
        assertEquals(1, putAndCompare(Short.MAX_VALUE, Short.MIN_VALUE, Short.class, maxSize));
        assertEquals(-1, putAndCompare(Short.MIN_VALUE, Short.MAX_VALUE, Short.class, maxSize));
        assertEquals(-2, putAndCompare((short)42, (short)16, Short.class, maxSize - 1));
    }

    /** */
    @Test
    public void testInt() throws Exception {
        testPutGet(ValueInt.get(-1), ValueInt.get(2), ValueInt.get(3));

        int maxSize = 1 + 4; // 1 byte header + 4 bytes value.

        assertEquals(1, putAndCompare(42, null, Integer.class, maxSize));
        assertEquals(1, putAndCompare(42, 16, Integer.class, maxSize));
        assertEquals(-1, putAndCompare(16, 42, Integer.class, maxSize));
        assertEquals(0, putAndCompare(42, 42, Integer.class, maxSize));
        assertEquals(0, putAndCompare(Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.class, maxSize));
        assertEquals(1, putAndCompare(Integer.MAX_VALUE, Integer.MIN_VALUE, Integer.class, maxSize));
        assertEquals(-1, putAndCompare(Integer.MIN_VALUE, Integer.MAX_VALUE, Integer.class, maxSize));
        assertEquals(-2, putAndCompare(42, 16, Integer.class, maxSize - 1));
    }

    /** */
    @Test
    public void testLong() throws Exception {
        testPutGet(ValueLong.get(-1), ValueLong.get(2), ValueLong.get(3));

        int maxSize = 1 + 8; // 1 byte header + 8 bytes value.

        assertEquals(1, putAndCompare((long)42, null, Long.class, maxSize));
        assertEquals(1, putAndCompare((long)42, (long)16, Long.class, maxSize));
        assertEquals(-1, putAndCompare((long)16, (long)42, Long.class, maxSize));
        assertEquals(0, putAndCompare((long)42, (long)42, Long.class, maxSize));
        assertEquals(0, putAndCompare(Long.MAX_VALUE, Long.MAX_VALUE, Long.class, maxSize));
        assertEquals(1, putAndCompare(Long.MAX_VALUE, Long.MIN_VALUE, Long.class, maxSize));
        assertEquals(-1, putAndCompare(Long.MIN_VALUE, Long.MAX_VALUE, Long.class, maxSize));
        assertEquals(-2, putAndCompare((long)42, (long)16, Long.class, maxSize - 1));
    }

    /** */
    @Test
    public void testFloat() throws Exception {
        testPutGet(ValueFloat.get(1.1f), ValueFloat.get(2.2f), ValueFloat.get(1.1f));

        int maxSize = 1 + 4; // 1 byte header + 4 bytes value.

        assertEquals(1, putAndCompare((float)42, null, Float.class, maxSize));
        assertEquals(1, putAndCompare((float)42, (float)16, Float.class, maxSize));
        assertEquals(-1, putAndCompare((float)16, (float)42, Float.class, maxSize));
        assertEquals(0, putAndCompare((float)42, (float)42, Float.class, maxSize));
        assertEquals(0, putAndCompare(Float.MAX_VALUE, Float.MAX_VALUE, Float.class, maxSize));
        assertEquals(1, putAndCompare(Float.MAX_VALUE, Float.MIN_VALUE, Float.class, maxSize));
        assertEquals(-1, putAndCompare(Float.MIN_VALUE, Float.MAX_VALUE, Float.class, maxSize));
        assertEquals(1, putAndCompare(Float.POSITIVE_INFINITY, Float.MAX_VALUE, Float.class, maxSize));
        assertEquals(-1, putAndCompare(Float.MAX_VALUE, Float.POSITIVE_INFINITY, Float.class, maxSize));
        assertEquals(0, putAndCompare(Float.POSITIVE_INFINITY, Float.POSITIVE_INFINITY, Float.class, maxSize));
        assertEquals(1, putAndCompare(Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, Float.class, maxSize));
        assertEquals(1, putAndCompare(Float.NaN, (float)16, Float.class, maxSize));
        assertEquals(1, putAndCompare(Float.NaN, Float.NEGATIVE_INFINITY, Float.class, maxSize));
        assertEquals(1, putAndCompare(Float.NaN, Float.POSITIVE_INFINITY, Float.class, maxSize));
        assertEquals(0, putAndCompare(Float.NaN, Float.NaN, Float.class, maxSize));
        assertEquals(-1, putAndCompare((float)16, Float.NaN, Float.class, maxSize));
        assertEquals(-1, putAndCompare(Float.NEGATIVE_INFINITY, Float.NaN, Float.class, maxSize));
        assertEquals(-1, putAndCompare(Float.POSITIVE_INFINITY, Float.NaN, Float.class, maxSize));
        assertEquals(-1, putAndCompare(Float.MAX_VALUE, Float.NaN, Float.class, maxSize));
        assertEquals(-1, putAndCompare(Float.MIN_VALUE, Float.NaN, Float.class, maxSize));
        assertEquals(-1, putAndCompare(Float.NEGATIVE_INFINITY, Float.MAX_VALUE, Float.class, maxSize));
        assertEquals(-2, putAndCompare((float)42, (float)16, Float.class, maxSize - 1));
    }

    /** */
    @Test
    public void testDouble() throws Exception {
        testPutGet(ValueDouble.get(1.1f), ValueDouble.get(2.2f), ValueDouble.get(1.1f));

        int maxSize = 1 + 8; // 1 byte header + 8 bytes value.

        assertEquals(1, putAndCompare((double)42, null, Double.class, maxSize));
        assertEquals(1, putAndCompare((double)42, (double)16, Double.class, maxSize));
        assertEquals(-1, putAndCompare((double)16, (double)42, Double.class, maxSize));
        assertEquals(0, putAndCompare((double)42, (double)42, Double.class, maxSize));
        assertEquals(0, putAndCompare(Double.MAX_VALUE, Double.MAX_VALUE, Double.class, maxSize));
        assertEquals(1, putAndCompare(Double.MAX_VALUE, Double.MIN_VALUE, Double.class, maxSize));
        assertEquals(-1, putAndCompare(Double.MIN_VALUE, Double.MAX_VALUE, Double.class, maxSize));
        assertEquals(1, putAndCompare(Double.POSITIVE_INFINITY, Double.MAX_VALUE, Double.class, maxSize));
        assertEquals(-1, putAndCompare(Double.MAX_VALUE, Double.POSITIVE_INFINITY, Double.class, maxSize));
        assertEquals(0, putAndCompare(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, Double.class, maxSize));
        assertEquals(1, putAndCompare(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.class, maxSize));
        assertEquals(1, putAndCompare(Double.NaN, (double)16, Double.class, maxSize));
        assertEquals(1, putAndCompare(Double.NaN, Double.NEGATIVE_INFINITY, Double.class, maxSize));
        assertEquals(1, putAndCompare(Double.NaN, Double.POSITIVE_INFINITY, Double.class, maxSize));
        assertEquals(0, putAndCompare(Double.NaN, Double.NaN, Double.class, maxSize));
        assertEquals(-1, putAndCompare((double)16, Double.NaN, Double.class, maxSize));
        assertEquals(-1, putAndCompare(Double.NEGATIVE_INFINITY, Double.NaN, Double.class, maxSize));
        assertEquals(-1, putAndCompare(Double.POSITIVE_INFINITY, Double.NaN, Double.class, maxSize));
        assertEquals(-1, putAndCompare(Double.MAX_VALUE, Double.NaN, Double.class, maxSize));
        assertEquals(-1, putAndCompare(Double.MIN_VALUE, Double.NaN, Double.class, maxSize));
        assertEquals(-1, putAndCompare(Double.NEGATIVE_INFINITY, Double.MAX_VALUE, Double.class, maxSize));
        assertEquals(-2, putAndCompare((double)42, (double)16, Double.class, maxSize - 1));
    }

    /** */
    @Test
    public void testDate() throws Exception {
        testPutGet(ValueDate.get(Date.valueOf("2017-02-20")),
            ValueDate.get(Date.valueOf("2017-02-21")),
            ValueDate.get(Date.valueOf("2017-02-19")));

        int maxSize = 1 + 8; // 1 byte header + 8 bytes value.

        assertEquals(1, putAndCompare(Date.valueOf("2017-02-24"), null, Date.class, maxSize));
        assertEquals(1, putAndCompare(Date.valueOf("2017-02-24"), Date.valueOf("2017-02-20"), Date.class, maxSize));
        assertEquals(-1, putAndCompare(Date.valueOf("2017-02-16"), Date.valueOf("2017-02-24"), Date.class, maxSize));
        assertEquals(0, putAndCompare(Date.valueOf("2017-02-24"), Date.valueOf("2017-02-24"), Date.class, maxSize));
        assertEquals(-2, putAndCompare(Date.valueOf("2017-02-24"), Date.valueOf("2017-02-20"), Date.class, maxSize - 1));
        assertEquals(-1, putAndCompare(new Date(Long.MIN_VALUE - DateTimeUtils.getTimeZoneOffsetMillis(TimeZone.getDefault(), Long.MIN_VALUE, 0)),
            new Date(Long.MAX_VALUE - DateTimeUtils.getTimeZoneOffsetMillis(TimeZone.getDefault(), Long.MAX_VALUE, 0)), Date.class, maxSize));
    }

    /** */
    @Test
    public void testTime() throws Exception {
        testPutGet(ValueTime.get(Time.valueOf("10:01:01")),
            ValueTime.get(Time.valueOf("11:02:02")),
            ValueTime.get(Time.valueOf("12:03:03")));

        int maxSize = 1 + 8; // 1 byte header + 8 bytes value.

        assertEquals(1, putAndCompare(Time.valueOf("4:20:00"), null, Time.class, maxSize));
        assertEquals(1, putAndCompare(Time.valueOf("4:20:00"), Time.valueOf("4:19:59"), Time.class, maxSize));
        assertEquals(-1, putAndCompare(Time.valueOf("4:19:59"), Time.valueOf("4:20:00"), Time.class, maxSize));
        assertEquals(0, putAndCompare(Time.valueOf("4:20:00"), Time.valueOf("4:20:00"), Time.class, maxSize));
        assertEquals(-2, putAndCompare(Time.valueOf("4:19:59"), Time.valueOf("4:20:00"), Time.class, maxSize - 1));
        assertEquals(-1, putAndCompare(Time.valueOf("00:00:00"), Time.valueOf("23:59:59"), Time.class, maxSize));
    }

    /** */
    @Test
    public void testTimestamp() throws Exception {
        testPutGet(ValueTimestamp.get(Timestamp.valueOf("2017-02-20 10:01:01")),
            ValueTimestamp.get(Timestamp.valueOf("2017-02-20 10:01:01")),
            ValueTimestamp.get(Timestamp.valueOf("2017-02-20 10:01:01")));

        int maxSize = 1 + 16; // 1 byte header + 16 bytes value.

        assertEquals(1, putAndCompare(Timestamp.valueOf("2017-02-20 4:20:00"), null, Timestamp.class, maxSize));
        assertEquals(1, putAndCompare(Timestamp.valueOf("2017-02-20 4:20:00"), Timestamp.valueOf("2017-02-19 4:20:00"), Timestamp.class, maxSize));
        assertEquals(-1, putAndCompare(Timestamp.valueOf("2017-02-19 4:20:00"), Timestamp.valueOf("2017-02-20 4:20:00"), Timestamp.class, maxSize));
        assertEquals(1, putAndCompare(Timestamp.valueOf("2017-02-20 4:20:00"), Timestamp.valueOf("2017-02-20 4:19:59"), Timestamp.class, maxSize));
        assertEquals(-1, putAndCompare(Timestamp.valueOf("2017-02-20 4:19:59"), Timestamp.valueOf("2017-02-20 4:20:00"), Timestamp.class, maxSize));
        assertEquals(0, putAndCompare(Timestamp.valueOf("2017-02-20 4:20:00"), Timestamp.valueOf("2017-02-20 4:20:00"), Timestamp.class, maxSize));
        assertEquals(-2, putAndCompare(Timestamp.valueOf("2017-02-20 4:19:59"), Timestamp.valueOf("2017-02-20 4:20:00"), Timestamp.class, maxSize - 1));
        assertEquals(-1, putAndCompare(Timestamp.valueOf("2017-02-20 00:00:00"), Timestamp.valueOf("2017-02-20 23:59:59"), Timestamp.class, maxSize));
        assertEquals(-1, putAndCompare(new Timestamp(Long.MIN_VALUE - DateTimeUtils.getTimeZoneOffsetMillis(TimeZone.getDefault(), Long.MAX_VALUE, 0)),
            new Timestamp(Long.MAX_VALUE - DateTimeUtils.getTimeZoneOffsetMillis(TimeZone.getDefault(), Long.MAX_VALUE, 0)), Timestamp.class, maxSize));
    }

    /** */
    @Test
    public void testUUID() throws Exception {
        testPutGet(ValueUuid.get(UUID.randomUUID().toString()),
            ValueUuid.get(UUID.randomUUID().toString()),
            ValueUuid.get(UUID.randomUUID().toString()));

        int maxSize = 1 + 16; // 1 byte header + 16 bytes value.

        assertEquals(1, putAndCompare(new UUID(42L, 16L), null, UUID.class, maxSize));
        assertEquals(1, putAndCompare(new UUID(42L, 16L), new UUID(42L, 10L), UUID.class, maxSize));
        assertEquals(-1, putAndCompare(new UUID(42L, 10L), new UUID(42L, 16L), UUID.class, maxSize));
        assertEquals(0, putAndCompare(new UUID(42L, 16L), new UUID(42L, 16L), UUID.class, maxSize));
        assertEquals(-1, putAndCompare(new UUID(42L, 16L), new UUID(48L, 10L), UUID.class, maxSize));
        assertEquals(1, putAndCompare(new UUID(48L, 10L), new UUID(42L, 16L), UUID.class, maxSize));
        assertEquals(1, putAndCompare(new UUID(48L, Long.MAX_VALUE), new UUID(48L, Long.MIN_VALUE), UUID.class, maxSize));
        assertEquals(-1, putAndCompare(new UUID(48L, Long.MIN_VALUE), new UUID(48L, Long.MAX_VALUE), UUID.class, maxSize));
        assertEquals(1, putAndCompare(new UUID(Long.MAX_VALUE, 10L), new UUID(Long.MIN_VALUE, 20L), UUID.class, maxSize));
        assertEquals(-1, putAndCompare(new UUID(Long.MIN_VALUE, 20L), new UUID(Long.MAX_VALUE, 10L), UUID.class, maxSize));
        assertEquals(-2, putAndCompare(new UUID(42L, 16L), new UUID(42L, 10L), UUID.class, maxSize - 1));
    }

    /** */
    @Test
    public void testJavaObjectHash() throws Exception {
        inlineObjHash = true;

        int maxSize = 1 + 4; // 1 byte header + 4 bytes value.

        assertEquals(1, putAndCompare(new TestPojo(42, 16L), null, TestPojo.class, maxSize));
        assertEquals(1, putAndCompare(new TestPojo(42, 16L), new TestPojo(16, 16L), TestPojo.class, maxSize));
        assertEquals(-1, putAndCompare(new TestPojo(16, 16L), new TestPojo(42, 16L), TestPojo.class, maxSize));
        assertEquals(-2, putAndCompare(new TestPojo(42, 16L), new TestPojo(42, 16L), TestPojo.class, maxSize));
        assertEquals(1, putAndCompare(new TestPojo(Integer.MAX_VALUE, 16L), new TestPojo(Integer.MIN_VALUE, 16L), TestPojo.class, maxSize));
        assertEquals(-1, putAndCompare(new TestPojo(Integer.MIN_VALUE, 16L), new TestPojo(Integer.MAX_VALUE, 16L), TestPojo.class, maxSize));
        assertEquals(-2, putAndCompare(new TestPojo(42, 16L), new TestPojo(16, 16L), TestPojo.class, maxSize - 1));
    }

    /** */
    private void testPutGet(Value v1, Value v2, Value v3) throws Exception {
        DataRegionConfiguration plcCfg = new DataRegionConfiguration().setInitialSize(1024 * MB)
            .setMaxSize(1024 * MB);

        PageMemory pageMem = new PageMemoryNoStoreImpl(log(),
            new UnsafeMemoryProvider(log()),
            null,
            PAGE_SIZE,
            plcCfg,
            new LongAdderMetric("NO_OP", null),
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

            InlineIndexColumnFactory factory = new InlineIndexColumnFactory(CompareMode.getInstance(CompareMode.DEFAULT, 1));

            AbstractInlineIndexColumn ih = (AbstractInlineIndexColumn)factory.createInlineHelper(new Column("", v1.getType()), false);

            off += ih.put(pageAddr, off, v1, max - off);
            off += ih.put(pageAddr, off, v2, max - off);
            off += ih.put(pageAddr, off, v3, max - off);

            Value v11 = ih.get(pageAddr, 0, max);
            Value v22 = ih.get(pageAddr, ih.fullSize(pageAddr, 0), max);

            assertEquals(v1.getObject(), v11.getObject());
            assertEquals(v2.getObject(), v22.getObject());

            assertEquals(0, ih.compare(pageAddr, 0, max, v1, ALWAYS_FAILS_COMPARATOR));
        }
        finally {
            if (page != 0L)
                pageMem.releasePage(CACHE_ID, pageId, page);

            pageMem.stop(true);
        }
    }

    /**
     * @param cnt String length.
     * @return Random string.
     */
    private String randomString(int cnt) {
        final char[] buffer = new char[cnt];

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        while (cnt-- != 0) {
            char ch;

            if (rnd.nextInt(100) > 3)
                ch = (char)(rnd.nextInt(95) + 32); // regular symbols
            else
                ch = (char)(rnd.nextInt(65407) + 127); // others symbols

            if (ch >= 56320 && ch <= 57343) {
                if (cnt == 0)
                    cnt++;
                else {
                    // low surrogate, insert high surrogate after putting it in
                    buffer[cnt] = ch;
                    cnt--;
                    buffer[cnt] = (char)(55296 + rnd.nextInt(128));
                }
            }
            else if (ch >= 55296 && ch <= 56191) {
                if (cnt == 0)
                    cnt++;
                else {
                    // high surrogate, insert low surrogate before putting it in
                    buffer[cnt] = (char)(56320 + rnd.nextInt(128));
                    cnt--;
                    buffer[cnt] = ch;
                }
            }
            else if (ch >= 56192 && ch <= 56319)
                // private high surrogate, no effing clue, so skip it
                cnt++;
            else
                buffer[cnt] = ch;
        }

        return new String(buffer);
    }

    /**
     *
     */
    private static class AlwaysFailsComparator implements Comparator<Value> {
        /** {@inheritDoc} */
        @Override public int compare(Value o1, Value o2) {
            throw new AssertionError("Optimized algorithm should be used.");
        }
    }

    /** Test class to verify java object inlining */
    private static class TestPojo implements Serializable {
        /** */
        private final int a;

        /** */
        private final long b;

        /** */
        public TestPojo(int a, long b) {
            this.a = a;
            this.b = b;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestPojo pojo = (TestPojo)o;

            return a == pojo.a && b == pojo.b;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return a;
        }
    }

    /**
     * Creates {@link Value} from provided value.
     *
     * @param val Value that should be wrapped by {@link Value}.
     * @param cls Class of the provided value.
     *
     * @return {@link Value} which wraps provided value.
     */
    private <T> Value wrap(T val, Class<T> cls) throws IgniteCheckedException {
        if (val == null)
            return ValueNull.INSTANCE;

        int type = typeByClass(cls);

        switch (type) {
            case Value.BOOLEAN:
                return ValueBoolean.get((Boolean)val);
            case Value.BYTE:
                return ValueByte.get((Byte)val);

            case Value.SHORT:
                return ValueShort.get((Short)val);

            case Value.INT:
                return ValueInt.get((Integer)val);

            case Value.LONG:
                return ValueLong.get((Long)val);

            case Value.FLOAT:
                return ValueFloat.get((Float)val);

            case Value.DOUBLE:
                return ValueDouble.get((Double)val);

            case Value.DATE:
                return ValueDate.get((Date)val);

            case Value.TIME:
                return ValueTime.get((Time)val);

            case Value.TIMESTAMP:
                return ValueTimestamp.get((Timestamp)val);

            case Value.UUID:
                return ValueUuid.get((UUID)val);

            case Value.STRING:
                return ValueString.get((String)val);

            case Value.STRING_FIXED:
                return ValueStringFixed.get(((FixedString)val).val);

            case Value.STRING_IGNORECASE:
                return ValueStringIgnoreCase.get(((IgnoreCaseString)val).val);

            case Value.JAVA_OBJECT:
                return ValueJavaObject.getNoCopy(marsh.marshal(val), null, null);
        }

        throw new IllegalStateException("Unknown value type: type=" + type + ", cls=" + cls.getName());
    }

    /**
     * Returns h2 value type code for provided class.
     */
    private int typeByClass(Class<?> cls) {
        if (Boolean.class.isAssignableFrom(cls))
            return Value.BOOLEAN;

        if (Byte.class.isAssignableFrom(cls))
            return Value.BYTE;

        if (Short.class.isAssignableFrom(cls))
            return Value.SHORT;

        if (Integer.class.isAssignableFrom(cls))
            return Value.INT;

        if (Long.class.isAssignableFrom(cls))
            return Value.LONG;

        if (Float.class.isAssignableFrom(cls))
            return Value.FLOAT;

        if (Double.class.isAssignableFrom(cls))
            return Value.DOUBLE;

        if (Date.class.isAssignableFrom(cls))
            return Value.DATE;

        if (Time.class.isAssignableFrom(cls))
            return Value.TIME;

        if (Timestamp.class.isAssignableFrom(cls))
            return Value.TIMESTAMP;

        if (UUID.class.isAssignableFrom(cls))
            return Value.UUID;

        if (String.class.isAssignableFrom(cls))
            return Value.STRING;

        if (FixedString.class.isAssignableFrom(cls))
            return Value.STRING_FIXED;

        if (IgnoreCaseString.class.isAssignableFrom(cls))
            return Value.STRING_IGNORECASE;

        return Value.JAVA_OBJECT;
    }

    /** Class wrapper, necessary to distinguish simple string and string with fixred length. */
    private static class FixedString {
        /** */
        private final String val;

        /** */
        public FixedString(String val) {
            this.val = val;
        }
    }

    /** Class wrapper, necessary to distinguish simple string and case insensitive strings. */
    private static class IgnoreCaseString {
        /** */
        private final String val;

        /** */
        public IgnoreCaseString(String val) {
            this.val = val;
        }
    }
}
