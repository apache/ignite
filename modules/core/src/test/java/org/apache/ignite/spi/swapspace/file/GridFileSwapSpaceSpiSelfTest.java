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

package org.apache.ignite.spi.swapspace.file;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.CIX1;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.spi.IgniteSpiCloseableIterator;
import org.apache.ignite.spi.swapspace.GridSwapSpaceSpiAbstractSelfTest;
import org.apache.ignite.spi.swapspace.SwapKey;
import org.apache.ignite.spi.swapspace.SwapSpaceSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;
import org.junit.Assert;

/**
 * Test for {@link FileSwapSpaceSpi}.
 */
public class GridFileSwapSpaceSpiSelfTest extends GridSwapSpaceSpiAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected SwapSpaceSpi spi() {
        FileSwapSpaceSpi s = new FileSwapSpaceSpi();

        s.setMaximumSparsity(0.05f);
        s.setWriteBufferSize(8 * 1024);

        return s;
    }

    /**
     * Tests if SPI works correctly with multithreaded writes.
     *
     * @throws Exception If failed.
     */
    public void testMultithreadedWrite() throws Exception {
        final AtomicLong valCntr = new AtomicLong();

        final SwapKey key = new SwapKey("key");

        final CountDownLatch wLatch = new CountDownLatch(1);

        final AtomicBoolean done = new AtomicBoolean();

        IgniteInternalFuture<?> wFut = multithreadedAsync(new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                while (!done.get()) {
                    long val = valCntr.incrementAndGet();

                    spi.store(null, key, Long.toString(val).getBytes(), context());

                    if (val == 1)
                        wLatch.countDown();
                }

                return null;
            }
        }, 8);

        wLatch.await();

        IgniteInternalFuture<?> rFut = multithreadedAsync(new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                while (valCntr.get() < 1000) {
                    byte[] val = spi.read(null, key, context());

                    assertNotNull(val);

                    long lval = Long.parseLong(new String(val));

                    assertTrue(lval <= valCntr.get());
                }

                return null;
            }
        }, 8);

        rFut.get();

        done.set(true);

        wFut.get();
    }

    /**
     * @param i Integer.
     * @return Swap key.
     */
    private SwapKey key(int i) {
        return new SwapKey(new KeyCacheObjectImpl(i, U.intToBytes(i)), i % 11, U.intToBytes(i));
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultithreadedOperations() throws Exception {
        final ConcurrentHashMap8<SwapKey, byte[]> map = new ConcurrentHashMap8<>();

        Random rnd = new Random();

        final int keys = 25000;

        int hash0 = 0;

        final int minValSize = 5;
        final int maxValSize = 9000; // More than write buffer size.

        for (int i = 0; i < keys; i++) {
            byte[] val = new byte[minValSize + rnd.nextInt(maxValSize - minValSize)];

            rnd.nextBytes(val);

            hash0 += i * Arrays.hashCode(val);

            assertNull(map.put(key(i), val));
        }

        assertEquals(keys, map.size());

        for (int i = 0; i < keys; i++)
            assertTrue(map.containsKey(key(i)));

        final String space = "test_space";

        final AtomicBoolean fin = new AtomicBoolean();

        final IgniteInternalFuture<?> fut = multithreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                Random rnd = new Random();

                while (!fin.get()) {
                    final SwapKey key = key(rnd.nextInt(keys));

                    switch(rnd.nextInt(13)) {
                        case 0: // store
                            byte[] val = map.remove(key);

                            if (val != null)
                                spi.store(space, key, val, context());

                            break;

                        case 1: // remove
                            spi.remove(space, key, new CIX1<byte[]>() {
                                @Override public void applyx(byte[] bytes) {
                                    if (bytes != null)
                                        assertNull(map.putIfAbsent(key, bytes));
                                }
                            }, context());

                            break;

                        case 2: // read
                            for (;;) {
                                val = spi.read(space, key, context());

                                if (val != null)
                                    break;

                                val = map.get(key);

                                if (val != null)
                                    break;
                            }

                            break;

                        case 3: // storeAll
                        case 4:
                        case 9:
                            Map<SwapKey, byte[]> m = new HashMap<>();

                            int cnt = 1 + rnd.nextInt(25);

                            for (int i = 0; i < cnt; i++) {
                                SwapKey k = key(rnd.nextInt(keys));

                                val = map.remove(k);

                                if (val != null)
                                    assertNull(m.put(k, val));
                            }

                            if (m.isEmpty())
                                break;

                            spi.storeAll(space, m, context());

                            break;

                        case 5: // readAll
                            HashSet<SwapKey> s = new HashSet<>();

                            cnt = 1 + rnd.nextInt(25);

                            for (int i = 0; i < cnt; i++) {
                                SwapKey k = key(rnd.nextInt(keys));

                                val = map.get(k);

                                if (val == null)
                                    s.add(k);
                            }

                            while (!s.isEmpty()) {
                                m = spi.readAll(space, s, context());

                                s.removeAll(m.keySet());

                                Iterator<SwapKey> iter = s.iterator();

                                while (iter.hasNext()) {
                                    SwapKey k = iter.next();

                                    if (map.containsKey(k))
                                        iter.remove();
                                }
                            }

                            break;

                        case 6: // iterateKeys
                            IgniteSpiCloseableIterator<Integer> kIt = spi.keyIterator(space, context());

                            if (kIt == null)
                                break;

                            while (kIt.hasNext())
                                assertNotNull(kIt.next());

                            kIt.close();

                            break;

                        case 7: // iterate
                            IgniteSpiCloseableIterator<Map.Entry<byte[], byte[]>> iter = spi.rawIterator(space);

                            if (iter == null)
                                break;

                            while (iter.hasNext()) {
                                Map.Entry<byte[], byte[]> entry = iter.next();

                                assertEquals(4, entry.getKey().length);

                                byte[] v = entry.getValue();

                                assertTrue(v.length >= minValSize && v.length < maxValSize);
                            }

                            iter.close();

                            break;

                        case 8: // iterate partitions
                            iter = spi.rawIterator(space, rnd.nextInt(11));

                            if (iter == null)
                                break;

                            while ( iter.hasNext()) {
                                Map.Entry<byte[], byte[]> entry = iter.next();

                                assertEquals(4, entry.getKey().length);

                                byte[] v = entry.getValue();

                                assertTrue(v.length >= minValSize && v.length < maxValSize);
                            }

                            iter.close();

                            break;

                        default: // removeAll
                            s = new HashSet<>();

                            cnt = 1 + rnd.nextInt(25);

                            for (int i = 0; i < cnt; i++) {
                                SwapKey k = key(rnd.nextInt(keys));

                                val = map.get(k);

                                if (val == null)
                                    s.add(k);
                            }

                            if (s.isEmpty())
                                break;

                            spi.removeAll(space, s, new IgniteBiInClosure<SwapKey, byte[]>() {
                                @Override public void apply(SwapKey k, byte[] bytes) {
                                    if (bytes != null)
                                        assertNull(map.putIfAbsent(k, bytes));
                                }
                            }, context());

                            break;
                    }
                }

                return null;
            }
        }, 39);

        Thread.sleep(60000);

        System.out.println("stopping");

        fin.set(true);

        fut.get();

        assertEquals(keys, map.size() + spi.count(space));

        int hash1 = 0;

        int cnt = 0;

        IgniteSpiCloseableIterator<Map.Entry<byte[], byte[]>> iter = spi.rawIterator(space);

        while (iter.hasNext()) {
            Map.Entry<byte[], byte[]> entry = iter.next();

            hash1 += U.bytesToInt(entry.getKey(), 0) * Arrays.hashCode(entry.getValue());

            cnt++;
        }

        assertEquals(cnt, spi.count(space));

        for (Map.Entry<SwapKey, byte[]> entry : map.entrySet()) {
            KeyCacheObject key = (KeyCacheObject)entry.getKey().key();

            hash1 += (Integer)key.value(null, false) * Arrays.hashCode(entry.getValue());
        }

        assertEquals(hash0, hash1);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testSaveValueLargeThenQueueSize() throws IgniteCheckedException {
        final String spaceName = "mySpace";
        final SwapKey key = new SwapKey("key");

        final byte[] val = new byte[FileSwapSpaceSpi.DFLT_QUE_SIZE * 2];
        Arrays.fill(val, (byte)1);

        IgniteInternalFuture<byte[]> fut = GridTestUtils.runAsync(new Callable<byte[]>() {
            @Override public byte[] call() throws Exception {
                return saveAndGet(spaceName, key, val);
            }
        });

        byte[] bytes = fut.get(10_000);

        Assert.assertArrayEquals(val, bytes);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testSaveValueLargeThenQueueSizeMultiThreaded() throws Exception {
        final String spaceName = "mySpace";

        final int threads = 5;

        long DURATION = 30_000;

        final int maxSize = FileSwapSpaceSpi.DFLT_QUE_SIZE * 2;

        final AtomicBoolean done = new AtomicBoolean();

        try {
            IgniteInternalFuture<?> fut = multithreadedAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    while (!done.get()) {
                        SwapKey key = new SwapKey(rnd.nextInt(1000));

                        spi.store(spaceName, key, new byte[rnd.nextInt(0, maxSize)], context());
                    }

                    return null;
                }
            }, threads, " async-put");

            Thread.sleep(DURATION);

            done.set(true);

            fut.get();
        }
        finally {
           done.set(true);
        }
    }

    /**
     * @param spaceName Space name.
     * @param key Key.
     * @param val Value.
     * @throws Exception If failed.
     * @return Read bytes.
     */
    private byte[] saveAndGet(final String spaceName, final SwapKey key, byte[] val) throws Exception {
        spi.store(spaceName, key, val, context());

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return spi.read(spaceName, key, context()) != null;
            }
        }, 10_000);

        byte[] res = spi.read(spaceName, key, context());

        assertNotNull(res);

        return res;
    }
}