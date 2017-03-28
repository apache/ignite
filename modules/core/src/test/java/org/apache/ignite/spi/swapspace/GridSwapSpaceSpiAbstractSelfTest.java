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

package org.apache.ignite.spi.swapspace;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.IgniteSpiCloseableIterator;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.events.EventType.EVT_SWAP_SPACE_DATA_READ;
import static org.apache.ignite.events.EventType.EVT_SWAP_SPACE_DATA_REMOVED;
import static org.apache.ignite.events.EventType.EVT_SWAP_SPACE_DATA_STORED;
import static org.junit.Assert.assertArrayEquals;

/**
 * Test for various {@link SwapSpaceSpi} implementations.
 */
public abstract class GridSwapSpaceSpiAbstractSelfTest extends GridCommonAbstractTest {
    /** Default swap space name. */
    private static final String DFLT_SPACE_NAME = "dflt-space";

    /** */
    protected static final String SPACE1 = "space1";

    /** */
    protected static final String SPACE2 = "space2";

    /** SPI to test. */
    protected SwapSpaceSpi spi;

    /**
     * @return New {@link SwapSpaceSpi} instance.
     */
    protected abstract SwapSpaceSpi spi();

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        spi = spi();

        getTestResources().inject(spi);

        spi.spiStart("");

        spi.clear(DFLT_SPACE_NAME);
    }

    /** @throws Exception If failed. */
    @Override protected void afterTest() throws Exception {
        spi.spiStop();
    }

    /**
     * @return Swap context.
     */
    protected SwapContext context() {
        return context(null);
    }

    /**
     * @param clsLdr Class loader.
     * @return Swap context.
     */
    private SwapContext context(@Nullable ClassLoader clsLdr) {
        SwapContext ctx = new SwapContext();

        ctx.classLoader(clsLdr != null ? clsLdr : getClass().getClassLoader());

        return ctx;
    }

    /**
     * @param s String.
     * @return Byte array.
     */
    protected byte[] str2ByteArray(String s) {
        return s.getBytes();
    }

    /**
     * Tests the Create-Read-Update-Delete operations with a simple key.
     *
     * @throws Exception If failed.
     */
    public void testSimpleCrud() throws Exception {
        assertEquals(0, spi.count(DFLT_SPACE_NAME));

        long key1 = 1;

        byte[] val1 = Long.toString(key1).getBytes();

        spi.store(DFLT_SPACE_NAME, new SwapKey(key1), val1, context());

        assertEquals(1, spi.count(DFLT_SPACE_NAME));

        assertArrayEquals(spi.read(DFLT_SPACE_NAME, new SwapKey(key1), context()), val1);

        final byte[] val2 = "newValue".getBytes();

        spi.store(DFLT_SPACE_NAME, new SwapKey(key1), val2, context());

        assertEquals(1, spi.count(DFLT_SPACE_NAME));

        assertArrayEquals(spi.read(DFLT_SPACE_NAME, new SwapKey(key1), context()), val2);

        spi.remove(DFLT_SPACE_NAME, new SwapKey(key1), new IgniteInClosure<byte[]>() {
            @Override public void apply(byte[] old) {
                assertArrayEquals(val2, old);
            }
        }, context());

        assertEquals(0, spi.count(DFLT_SPACE_NAME));
    }

    /**
     * Tests the Create-Read-Update-Delete operations with a simple key
     * and different spaces.
     *
     * @throws Exception If failed.
     */
    public void testSimpleCrudDifferentSpaces() throws Exception {
        String space1 = SPACE1;

        spi.clear(space1);

        String space2 = SPACE2;

        spi.clear(space2);

        assertEquals(0, spi.count(space1));

        assertEquals(0, spi.count(space2));

        long key1 = 1;

        final byte[] val1 = Long.toString(key1).getBytes();

        spi.store(space1, new SwapKey(key1), val1, context());

        assertEquals(1, spi.count(space1));

        assertEquals(0, spi.count(space2));

        spi.store(space2, new SwapKey(key1), val1, context());

        assertEquals(1, spi.count(space1));

        assertEquals(1, spi.count(space2));

        assertArrayEquals(spi.read(space1, new SwapKey(key1), context()), val1);

        assertArrayEquals(spi.read(space2, new SwapKey(key1), context()), val1);

        long key2 = 2;

        byte[] val2 = Long.toString(key2).getBytes();

        spi.store(space1, new SwapKey(key2), val2, context());

        assertEquals(2, spi.count(space1));

        assertEquals(1, spi.count(space2));

        assertArrayEquals(spi.read(space1, new SwapKey(key2), context()), val2);

        assertNull(spi.read(space2, new SwapKey(key2), context()));

        final byte[] val12 = "newValue".getBytes();

        spi.store(space1, new SwapKey(key1), val12, context());

        assertEquals(2, spi.count(space1));

        assertEquals(1, spi.count(space2));

        assertArrayEquals(spi.read(space1, new SwapKey(key1), context()), val12);

        assertArrayEquals(spi.read(space2, new SwapKey(key1), context()), val1);

        spi.remove(space1, new SwapKey(key1), new IgniteInClosure<byte[]>() {
            @Override public void apply(byte[] old) {
                assertArrayEquals(val12, old);
            }
        }, context());

        assertEquals(1, spi.count(space1));

        assertEquals(1, spi.count(space2));

        spi.remove(space2, new SwapKey(key1), new IgniteInClosure<byte[]>() {
            @Override public void apply(byte[] old) {
                assertArrayEquals(val1, old);
            }
        }, context());

        assertEquals(1, spi.count(space1));

        assertEquals(0, spi.count(space2));
    }

    /**
     * Tests the Create-Update-Delete operations with a key batches.
     *
     * @throws Exception If failed.
     */
    public void testBatchCrud() throws Exception {
        assertEquals(0, spi.count(DFLT_SPACE_NAME));

        final Map<SwapKey, byte[]> batch = new HashMap<>();

        int batchSize = 10;

        // Generate initial values.
        for (int i = 0; i < batchSize; i++)
            batch.put(new SwapKey(i), Integer.toString(i).getBytes());

        spi.storeAll(DFLT_SPACE_NAME, batch, context());

        assertEquals(batchSize, spi.count(DFLT_SPACE_NAME));

        Map<SwapKey, byte[]> read = spi.readAll(DFLT_SPACE_NAME, batch.keySet(), context());

        // Check all entries are as expected.
        assertTrue(F.forAll(read, new P1<Map.Entry<SwapKey, byte[]>>() {
            @Override public boolean apply(Map.Entry<SwapKey, byte[]> e) {
                return Arrays.equals(batch.get(e.getKey()), e.getValue());
            }
        }));

        // Generate new values.
        for (int i = 0; i < batchSize; i++)
            batch.put(new SwapKey(i), Integer.toString(i + 1).getBytes());

        spi.storeAll(DFLT_SPACE_NAME, batch, context());

        assertEquals(batchSize, spi.count(DFLT_SPACE_NAME));

        read = spi.readAll(DFLT_SPACE_NAME, batch.keySet(), context());

        // Check all entries are as expected.
        assertTrue(F.forAll(read, new P1<Map.Entry<SwapKey, byte[]>>() {
            @Override public boolean apply(Map.Entry<SwapKey, byte[]> e) {
                return Arrays.equals(batch.get(e.getKey()), e.getValue());
            }
        }));

        spi.removeAll(DFLT_SPACE_NAME, batch.keySet(), null, context());

        assertEquals(0, spi.count(DFLT_SPACE_NAME));
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeleteIfNotPersist() throws Exception {
        spi.store(SPACE1, new SwapKey("key1"), "value1".getBytes(), context());

        assertArrayEquals("value1".getBytes(), spi.read(SPACE1, new SwapKey("key1"), context()));
    }

    /**
     * @throws Exception If failed.
     */
    public void testStoreReadRemove() throws Exception {
        int cnt = 5;

        final CountDownLatch storeLatch = new CountDownLatch(cnt);
        final CountDownLatch readLatch = new CountDownLatch(cnt);
        final CountDownLatch rmvLatch = new CountDownLatch(cnt);

        spi.setListener(new SwapSpaceSpiListener() {
            @Override public void onSwapEvent(int evtType, @Nullable String spaceName, @Nullable byte[] keyBytes,
                @Nullable byte[] valBytes) {
                info("Received event: " + evtType);

                if (evtType == EVT_SWAP_SPACE_DATA_STORED)
                    storeLatch.countDown();

                else if (evtType == EVT_SWAP_SPACE_DATA_READ)
                    readLatch.countDown();

                else if (evtType == EVT_SWAP_SPACE_DATA_REMOVED)
                    rmvLatch.countDown();

                else
                    assert false : "Unexpected event type: " + evtType;
            }
        });

        for (int i = 0; i < cnt; i++)
            assertNull(spi.read(SPACE1, new SwapKey("key" + i), context()));

        for (int i = 0; i < cnt; i++)
            spi.store(SPACE1, new SwapKey("key" + i), str2ByteArray("value" + i), context());

        assert storeLatch.await(5000, MILLISECONDS);

        for (int i = 0; i < cnt; i++)
            assertArrayEquals(str2ByteArray("value" + i), spi.read(SPACE1, new SwapKey("key" + i), context()));

        assert readLatch.await(5000, MILLISECONDS);

        for (int i = 0; i < cnt; i++) {
            final int tmp = i;

            spi.remove(SPACE1, new SwapKey("key" + i), new CI1<byte[]>() {
                @Override public void apply(byte[] arr) {
                    assertArrayEquals(str2ByteArray("value" + tmp), arr);

                    info("Removed correct value for: key" + tmp);
                }
            }, context());
        }

        assert rmvLatch.await(10000, MILLISECONDS);

        for (int i = 0; i < cnt; i++)
            assertNull(spi.read(SPACE1, new SwapKey("key" + i), context()));
    }


    /**
     * @throws Exception If failed.
     */
    public void testStoreReadRemoveNulls() throws Exception {
        int cnt = 5;

        final CountDownLatch storeLatch = new CountDownLatch(cnt);
        final CountDownLatch readLatch = new CountDownLatch(cnt);
        final CountDownLatch rmvLatch = new CountDownLatch(cnt);

        spi.setListener(new SwapSpaceSpiListener() {
            @Override public void onSwapEvent(int evtType, @Nullable String spaceName, @Nullable byte[] keyBytes,
                @Nullable byte[] valBytes) {
                info("Received event: " + evtType);

                if (evtType == EVT_SWAP_SPACE_DATA_STORED)
                    storeLatch.countDown();

                else if (evtType == EVT_SWAP_SPACE_DATA_READ)
                    readLatch.countDown();

                else if (evtType == EVT_SWAP_SPACE_DATA_REMOVED)
                    rmvLatch.countDown();

                else
                    assert false : "Unexpected event type: " + evtType;
            }
        });

        for (int i = 0; i < cnt; i++)
            assertNull(spi.read(SPACE1, new SwapKey("key" + i), context()));

        for (int i = 0; i < cnt; i++)
            spi.store(SPACE1, new SwapKey("key" + i), null, context());

        assert storeLatch.await(5000, MILLISECONDS);

        for (int i = 0; i < cnt; i++)
            assertNull(spi.read(SPACE1, new SwapKey("key" + i), context()));

        assert readLatch.await(5000, MILLISECONDS);

        for (int i = 0; i < cnt; i++) {
            final int tmp = i;

            spi.remove(SPACE1, new SwapKey("key" + i), new CI1<byte[]>() {
                @Override public void apply(byte[] arr) {
                    assertNull(arr);

                    info("Removed correct value for: key" + tmp);
                }
            }, context());
        }

        assert rmvLatch.await(10000, MILLISECONDS);

        for (int i = 0; i < cnt; i++)
            assertNull(spi.read(SPACE1, new SwapKey("key" + i), context()));
    }

    /**
     * @throws Exception If failed.
     */
    public void testCollisions() throws Exception {
        int cnt = 5;

        final CountDownLatch storeLatch = new CountDownLatch(cnt);
        final CountDownLatch readLatch = new CountDownLatch(cnt);
        final CountDownLatch rmvLatch = new CountDownLatch(cnt);

        spi.setListener(new SwapSpaceSpiListener() {
            @Override public void onSwapEvent(int evtType, @Nullable String spaceName, @Nullable byte[] keyBytes,
                @Nullable byte[] valBytes) {
                info("Received event: " + evtType);

                if (evtType == EVT_SWAP_SPACE_DATA_STORED)
                    storeLatch.countDown();

                else if (evtType == EVT_SWAP_SPACE_DATA_READ)
                    readLatch.countDown();

                else if (evtType == EVT_SWAP_SPACE_DATA_REMOVED)
                    rmvLatch.countDown();

                else
                    assert false : "Unexpected event type: " + evtType;
            }
        });

        List<Integer> keys = new ArrayList<>(cnt);

        final Map<Integer, String> entries = new HashMap<>();

        for (int i = 0; i < cnt; i++) {
            String val = "value" + i;

            spi.store(SPACE1, new SwapKey(new Key(i)), str2ByteArray(val), context());

            keys.add(i);

            entries.put(i, val);
        }

        assert storeLatch.await(5000, MILLISECONDS) : "Count: " + storeLatch.getCount();

        for (int i = 0; i < cnt; i++)
            assertArrayEquals(entries.get(i).getBytes(),
                spi.read(SPACE1, new SwapKey(new Key(i)), context()));

        assert readLatch.await(5000, MILLISECONDS) : "Count: " + readLatch.getCount();

        Collections.shuffle(keys);

        for (final Integer key : keys) {
            spi.remove(SPACE1, new SwapKey(new Key(key)), new CI1<byte[]>() {
                @Override public void apply(byte[] arr) {
                    assertArrayEquals(entries.get(key).getBytes(), arr);

                    info("Removed correct entry for key: " + key);
                }
            }, context());
        }

        assert rmvLatch.await(5000, MILLISECONDS) : "Count: " + rmvLatch.getCount();

        for (final Integer key : keys)
            assertNull(spi.read(SPACE1, new SwapKey(new Key(key)), context()));
    }


    /**
     * @throws Exception If failed.
     */
    public void testIteration() throws Exception {
        spi.clear(SPACE1);

        int cnt = 10;

        for (int i = 0; i < cnt; i++)
            spi.store(SPACE1, new SwapKey("key" + i, i), str2ByteArray("value" + i), context());

        for (int i = 0; i < cnt; i++)
            assertArrayEquals(str2ByteArray("value" + i),
                spi.read(SPACE1, new SwapKey("key" + i, i), context()));

        try (IgniteSpiCloseableIterator<Map.Entry<byte[], byte[]>> iter = spi.rawIterator(SPACE1)) {
            assertNotNull(iter);

            int i = 0;

            while (iter.hasNext()) {
                Map.Entry<byte[], byte[]> next = iter.next();

                String key = getTestResources().getMarshaller().unmarshal(next.getKey(), null);

                info("Got from iterator [key=" + key + ", val=" + new String(next.getValue()));

                i++;

                iter.remove();
            }

            assertEquals(10, i);
        }

        assertEquals(0, spi.count(SPACE1));
    }

    /**
     * @throws Exception If failed.
     */
    public void testIterationOverPartition() throws Exception {
        spi.store(SPACE1, new SwapKey("key", 0), str2ByteArray("value"), context());

        spi.clear(SPACE1);

        int cnt = 10;

        for (int i = 0; i < cnt; i++)
            spi.store(SPACE1, new SwapKey("key" + i, i), str2ByteArray("value" + i), context());

        for (int i = 0; i < cnt; i++)
            assertArrayEquals(str2ByteArray("value" + i),
                spi.read(SPACE1, new SwapKey("key" + i, i), context()));

        try (IgniteSpiCloseableIterator<Map.Entry<byte[], byte[]>> iter = spi.rawIterator(SPACE1, 5)) {
            assertNotNull(iter);

            int i = 0;

            while (iter.hasNext()) {
                Map.Entry<byte[], byte[]> next = iter.next();

                String key = getTestResources().getMarshaller().unmarshal(next.getKey(), null);

                info("Got from iterator [key=" + key + ", val=" + new String(next.getValue()));

                assert "key5".equals(key);

                iter.remove();

                assertNull(spi.read(SPACE1, new SwapKey(key, 5), context()));

                i++;
            }

            assertEquals(1, i);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSwapIterator() throws Exception {
        spi.store(SPACE1, new SwapKey("key", 0), str2ByteArray("value"), context());

        spi.clear(SPACE1);

        int cnt = 10;

        for (int i = 0; i < cnt; i++)
            spi.store(SPACE1, new SwapKey("key" + i, i), str2ByteArray("value" + i), context());

        IgniteSpiCloseableIterator<Map.Entry<byte[], byte[]>> iter = spi.rawIterator(SPACE1);

        assertNotNull(iter);

        iter.close();

        try {
            iter.next();

            assert false;
        }
        catch (NoSuchElementException e) {
            info("Caught expected exception (illegal state): " + e);
        }
    }

    /**
     *
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static class TestValue implements Serializable {
        /** */
        private String val = "test-" + System.currentTimeMillis();

        /**
         * @return Value
         */
        public String getValue() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            return obj instanceof TestValue && val.equals(((TestValue)obj).val);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TestValue.class, this);
        }
    }

    /**
     * Key.
     */
    private static class Key implements Serializable {
        /** Index. */
        private final int i;

        /**
         * @param i Index.
         */
        Key(int i) {
            this.i = i;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof Key))
                return false;

            Key key = (Key)o;

            return i == key.i;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return 1; // 100% collision.
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Key: " + i;
        }
    }
}
