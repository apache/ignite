/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.swapspace;

import org.gridgain.grid.lang.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.*;
import static org.gridgain.grid.events.GridEventType.*;
import static org.junit.Assert.*;

/**
 * Test for various {@link GridSwapSpaceSpi} implementations.
 */
public abstract class GridSwapSpaceSpiAbstractSelfTest extends GridCommonAbstractTest {
    /** Default swap space name. */
    private static final String DFLT_SPACE_NAME = "dflt-space";

    /** */
    protected static final String SPACE1 = "space1";

    /** */
    protected static final String SPACE2 = "space2";

    /** SPI to test. */
    protected GridSwapSpaceSpi spi;

    /**
     * @return New {@link GridSwapSpaceSpi} instance.
     */
    protected abstract GridSwapSpaceSpi spi();

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        U.setWorkDirectory(null, U.getGridGainHome());

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
    protected GridSwapContext context() {
        return context(null);
    }

    /**
     * @param clsLdr Class loader.
     * @return Swap context.
     */
    private GridSwapContext context(@Nullable ClassLoader clsLdr) {
        GridSwapContext ctx = new GridSwapContext();

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

        spi.store(DFLT_SPACE_NAME, new GridSwapKey(key1), val1, context());

        assertEquals(1, spi.count(DFLT_SPACE_NAME));

        assertArrayEquals(spi.read(DFLT_SPACE_NAME, new GridSwapKey(key1), context()), val1);

        final byte[] val2 = "newValue".getBytes();

        spi.store(DFLT_SPACE_NAME, new GridSwapKey(key1), val2, context());

        assertEquals(1, spi.count(DFLT_SPACE_NAME));

        assertArrayEquals(spi.read(DFLT_SPACE_NAME, new GridSwapKey(key1), context()), val2);

        spi.remove(DFLT_SPACE_NAME, new GridSwapKey(key1), new GridInClosure<byte[]>() {
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

        spi.store(space1, new GridSwapKey(key1), val1, context());

        assertEquals(1, spi.count(space1));

        assertEquals(0, spi.count(space2));

        spi.store(space2, new GridSwapKey(key1), val1, context());

        assertEquals(1, spi.count(space1));

        assertEquals(1, spi.count(space2));

        assertArrayEquals(spi.read(space1, new GridSwapKey(key1), context()), val1);

        assertArrayEquals(spi.read(space2, new GridSwapKey(key1), context()), val1);

        long key2 = 2;

        byte[] val2 = Long.toString(key2).getBytes();

        spi.store(space1, new GridSwapKey(key2), val2, context());

        assertEquals(2, spi.count(space1));

        assertEquals(1, spi.count(space2));

        assertArrayEquals(spi.read(space1, new GridSwapKey(key2), context()), val2);

        assertNull(spi.read(space2, new GridSwapKey(key2), context()));

        final byte[] val12 = "newValue".getBytes();

        spi.store(space1, new GridSwapKey(key1), val12, context());

        assertEquals(2, spi.count(space1));

        assertEquals(1, spi.count(space2));

        assertArrayEquals(spi.read(space1, new GridSwapKey(key1), context()), val12);

        assertArrayEquals(spi.read(space2, new GridSwapKey(key1), context()), val1);

        spi.remove(space1, new GridSwapKey(key1), new GridInClosure<byte[]>() {
            @Override public void apply(byte[] old) {
                assertArrayEquals(val12, old);
            }
        }, context());

        assertEquals(1, spi.count(space1));

        assertEquals(1, spi.count(space2));

        spi.remove(space2, new GridSwapKey(key1), new GridInClosure<byte[]>() {
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

        final Map<GridSwapKey, byte[]> batch = new HashMap<>();

        int batchSize = 10;

        // Generate initial values.
        for (int i = 0; i < batchSize; i++)
            batch.put(new GridSwapKey(i), Integer.toString(i).getBytes());

        spi.storeAll(DFLT_SPACE_NAME, batch, context());

        assertEquals(batchSize, spi.count(DFLT_SPACE_NAME));

        Map<GridSwapKey, byte[]> read = spi.readAll(DFLT_SPACE_NAME, batch.keySet(), context());

        // Check all entries are as expected.
        assertTrue(F.forAll(read, new P1<Map.Entry<GridSwapKey, byte[]>>() {
            @Override public boolean apply(Map.Entry<GridSwapKey, byte[]> e) {
                return Arrays.equals(batch.get(e.getKey()), e.getValue());
            }
        }));

        // Generate new values.
        for (int i = 0; i < batchSize; i++)
            batch.put(new GridSwapKey(i), Integer.toString(i + 1).getBytes());

        spi.storeAll(DFLT_SPACE_NAME, batch, context());

        assertEquals(batchSize, spi.count(DFLT_SPACE_NAME));

        read = spi.readAll(DFLT_SPACE_NAME, batch.keySet(), context());

        // Check all entries are as expected.
        assertTrue(F.forAll(read, new P1<Map.Entry<GridSwapKey, byte[]>>() {
            @Override public boolean apply(Map.Entry<GridSwapKey, byte[]> e) {
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
        spi.store(SPACE1, new GridSwapKey("key1"), "value1".getBytes(), context());

        assertArrayEquals("value1".getBytes(), spi.read(SPACE1, new GridSwapKey("key1"), context()));
    }

    /**
     * @throws Exception If failed.
     */
    public void testStoreReadRemove() throws Exception {
        int cnt = 5;

        final CountDownLatch storeLatch = new CountDownLatch(cnt);
        final CountDownLatch readLatch = new CountDownLatch(cnt);
        final CountDownLatch rmvLatch = new CountDownLatch(cnt);

        spi.setListener(new GridSwapSpaceSpiListener() {
            @Override public void onSwapEvent(int evtType, @Nullable String spaceName, @Nullable byte[] keyBytes) {
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
            assertNull(spi.read(SPACE1, new GridSwapKey("key" + i), context()));

        for (int i = 0; i < cnt; i++)
            spi.store(SPACE1, new GridSwapKey("key" + i), str2ByteArray("value" + i), context());

        assert storeLatch.await(5000, MILLISECONDS);

        for (int i = 0; i < cnt; i++)
            assertArrayEquals(str2ByteArray("value" + i), spi.read(SPACE1, new GridSwapKey("key" + i), context()));

        assert readLatch.await(5000, MILLISECONDS);

        for (int i = 0; i < cnt; i++) {
            final int tmp = i;

            spi.remove(SPACE1, new GridSwapKey("key" + i), new CI1<byte[]>() {
                @Override public void apply(byte[] arr) {
                    assertArrayEquals(str2ByteArray("value" + tmp), arr);

                    info("Removed correct value for: key" + tmp);
                }
            }, context());
        }

        assert rmvLatch.await(10000, MILLISECONDS);

        for (int i = 0; i < cnt; i++)
            assertNull(spi.read(SPACE1, new GridSwapKey("key" + i), context()));
    }


    /**
     * @throws Exception If failed.
     */
    public void testStoreReadRemoveNulls() throws Exception {
        int cnt = 5;

        final CountDownLatch storeLatch = new CountDownLatch(cnt);
        final CountDownLatch readLatch = new CountDownLatch(cnt);
        final CountDownLatch rmvLatch = new CountDownLatch(cnt);

        spi.setListener(new GridSwapSpaceSpiListener() {
            @Override public void onSwapEvent(int evtType, @Nullable String spaceName, @Nullable byte[] keyBytes) {
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
            assertNull(spi.read(SPACE1, new GridSwapKey("key" + i), context()));

        for (int i = 0; i < cnt; i++)
            spi.store(SPACE1, new GridSwapKey("key" + i), null, context());

        assert storeLatch.await(5000, MILLISECONDS);

        for (int i = 0; i < cnt; i++)
            assertNull(spi.read(SPACE1, new GridSwapKey("key" + i), context()));

        assert readLatch.await(5000, MILLISECONDS);

        for (int i = 0; i < cnt; i++) {
            final int tmp = i;

            spi.remove(SPACE1, new GridSwapKey("key" + i), new CI1<byte[]>() {
                @Override public void apply(byte[] arr) {
                    assertNull(arr);

                    info("Removed correct value for: key" + tmp);
                }
            }, context());
        }

        assert rmvLatch.await(10000, MILLISECONDS);

        for (int i = 0; i < cnt; i++)
            assertNull(spi.read(SPACE1, new GridSwapKey("key" + i), context()));
    }

    /**
     * @throws Exception If failed.
     */
    public void testCollisions() throws Exception {
        int cnt = 5;

        final CountDownLatch storeLatch = new CountDownLatch(cnt);
        final CountDownLatch readLatch = new CountDownLatch(cnt);
        final CountDownLatch rmvLatch = new CountDownLatch(cnt);

        spi.setListener(new GridSwapSpaceSpiListener() {
            @Override public void onSwapEvent(int evtType, @Nullable String spaceName, @Nullable byte[] keyBytes) {
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

            spi.store(SPACE1, new GridSwapKey(new Key(i)), str2ByteArray(val), context());

            keys.add(i);

            entries.put(i, val);
        }

        assert storeLatch.await(5000, MILLISECONDS) : "Count: " + storeLatch.getCount();

        for (int i = 0; i < cnt; i++)
            assertArrayEquals(entries.get(i).getBytes(),
                spi.read(SPACE1, new GridSwapKey(new Key(i)), context()));

        assert readLatch.await(5000, MILLISECONDS) : "Count: " + readLatch.getCount();

        Collections.shuffle(keys);

        for (final Integer key : keys) {
            spi.remove(SPACE1, new GridSwapKey(new Key(key)), new CI1<byte[]>() {
                @Override public void apply(byte[] arr) {
                    assertArrayEquals(entries.get(key).getBytes(), arr);

                    info("Removed correct entry for key: " + key);
                }
            }, context());
        }

        assert rmvLatch.await(5000, MILLISECONDS) : "Count: " + rmvLatch.getCount();

        for (final Integer key : keys)
            assertNull(spi.read(SPACE1, new GridSwapKey(new Key(key)), context()));
    }


    /**
     * @throws Exception If failed.
     */
    public void testIteration() throws Exception {
        spi.clear(SPACE1);

        int cnt = 10;

        for (int i = 0; i < cnt; i++)
            spi.store(SPACE1, new GridSwapKey("key" + i, i), str2ByteArray("value" + i), context());

        for (int i = 0; i < cnt; i++)
            assertArrayEquals(str2ByteArray("value" + i),
                spi.read(SPACE1, new GridSwapKey("key" + i, i), context()));

        try (GridSpiCloseableIterator<Map.Entry<byte[], byte[]>> iter = spi.rawIterator(SPACE1)) {
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
        spi.store(SPACE1, new GridSwapKey("key", 0), str2ByteArray("value"), context());

        spi.clear(SPACE1);

        int cnt = 10;

        for (int i = 0; i < cnt; i++)
            spi.store(SPACE1, new GridSwapKey("key" + i, i), str2ByteArray("value" + i), context());

        for (int i = 0; i < cnt; i++)
            assertArrayEquals(str2ByteArray("value" + i),
                spi.read(SPACE1, new GridSwapKey("key" + i, i), context()));

        try (GridSpiCloseableIterator<Map.Entry<byte[], byte[]>> iter = spi.rawIterator(SPACE1, 5)) {
            assertNotNull(iter);

            int i = 0;

            while (iter.hasNext()) {
                Map.Entry<byte[], byte[]> next = iter.next();

                String key = getTestResources().getMarshaller().unmarshal(next.getKey(), null);

                info("Got from iterator [key=" + key + ", val=" + new String(next.getValue()));

                assert "key5".equals(key);

                iter.remove();

                assertNull(spi.read(SPACE1, new GridSwapKey(key, 5), context()));

                i++;
            }

            assertEquals(1, i);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSwapIterator() throws Exception {
        spi.store(SPACE1, new GridSwapKey("key", 0), str2ByteArray("value"), context());

        spi.clear(SPACE1);

        int cnt = 10;

        for (int i = 0; i < cnt; i++)
            spi.store(SPACE1, new GridSwapKey("key" + i, i), str2ByteArray("value" + i), context());

        GridSpiCloseableIterator<Map.Entry<byte[], byte[]>> iter = spi.rawIterator(SPACE1);

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
    private static class Key {
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
