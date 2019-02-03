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

package org.apache.ignite.internal.processors.cache.persistence.wal;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.wal.SegmentedRingByteBuffer.BufferMode.DIRECT;
import static org.apache.ignite.internal.processors.cache.persistence.wal.SegmentedRingByteBuffer.BufferMode.ONHEAP;

/**
 *
 */
public class SegmentedRingByteBufferTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAligned() throws Exception {
        doTestAligned(ONHEAP);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAlignedDirect() throws Exception {
        doTestAligned(DIRECT);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNotAligned() throws Exception {
        doTestNotAligned(ONHEAP);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNotAlignedDirect() throws Exception {
        doTestNotAligned(DIRECT);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNoOverflowMultiThreaded() throws Exception {
        doTestNoOverflowMultiThreaded(ONHEAP);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNoOverflowMultiThreadedDirect() throws Exception {
        doTestNoOverflowMultiThreaded(DIRECT);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultiThreaded() throws Exception {
        doTestMultiThreaded(ONHEAP);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultiThreadedDirect() throws Exception {
        doTestMultiThreaded(DIRECT);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultiThreaded2() throws Exception {
        doTestMultiThreaded2(ONHEAP);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultiThreadedDirect2() throws Exception {
        doTestMultiThreaded2(DIRECT);
    }

    /**
     * @param mode Mode.
     */
    private void doTestAligned(SegmentedRingByteBuffer.BufferMode mode) {
        int cap = 128;

        int size = 8;

        SegmentedRingByteBuffer buf = new SegmentedRingByteBuffer(cap, Long.MAX_VALUE, mode);

        assertNull(buf.poll());

        // Head and tail in initial state.
        for (int j = 0; j < 2; j++) {
            for (int i = 0; i < cap / size; i++) {
                SegmentedRingByteBuffer.WriteSegment seg = buf.offer(size);

                ByteBuffer bbuf = seg.buffer();

                assertEquals(size * i, bbuf.position());
                assertEquals(size * (i + 1), bbuf.limit());

                bbuf.putLong(i + (j * 10));

                seg.release();
            }

            assertNull(buf.offer(size));

            List<SegmentedRingByteBuffer.ReadSegment> segs = buf.poll();

            ByteBuffer bbuf = segs.get(0).buffer();

            assertEquals(cap, bbuf.remaining());

            for (int i = 0; i < cap / size; i++)
                assertEquals(i + (j * 10), bbuf.getLong());

            segs.get(0).release();

            assertEquals(0, bbuf.remaining());
            assertNull(buf.poll());
        }

        // Move tail.
        for (int i = 0; i < 2; i++) {
            SegmentedRingByteBuffer.WriteSegment seg = buf.offer(size);

            ByteBuffer bbuf = seg.buffer();

            assertEquals(size * i, bbuf.position());
            assertEquals(size * (i + 1), bbuf.limit());

            bbuf.putLong(i);

            seg.release();
        }

        // Move head to tail.
        List<SegmentedRingByteBuffer.ReadSegment> segs = buf.poll();

        ByteBuffer bbuf = segs.get(0).buffer();

        assertEquals(size * 2, bbuf.remaining());

        for (int i = 0; i < 2; i++)
            assertEquals(i, bbuf.getLong());

        segs.get(0).release();

        assertEquals(0, bbuf.remaining());
        assertNull(buf.poll());
    }

    /**
     * @param mode Mode.
     */
    private void doTestNotAligned(SegmentedRingByteBuffer.BufferMode mode) {
        int size = 8;

        int cap = 32 - size / 2; // 3.5 long values.

        SegmentedRingByteBuffer buf = new SegmentedRingByteBuffer(cap, Long.MAX_VALUE, mode);

        assertNull(buf.poll());

        // Write 2 segments.
        SegmentedRingByteBuffer.WriteSegment wseg;
        List<SegmentedRingByteBuffer.ReadSegment> rsegs;
        ByteBuffer bbuf;

        wseg = buf.offer(size);
        bbuf = wseg.buffer();

        bbuf.putLong(1);

        wseg.release();

        wseg = buf.offer(size);

        bbuf = wseg.buffer();

        bbuf.putLong(2);

        wseg.release();

        // Read 2 segments.
        rsegs = buf.poll();
        bbuf = rsegs.get(0).buffer();

        assertEquals(1, bbuf.getLong());
        assertEquals(2, bbuf.getLong());

        rsegs.get(0).release();

        assertNull(buf.poll());

        // Write 2 segments.
        wseg = buf.offer(size);
        bbuf = wseg.buffer();

        bbuf.putLong(3);

        wseg.release();

        // This one will overflow buffer.
        wseg = buf.offer(size);
        bbuf = wseg.buffer();

        bbuf.putLong(4);

        wseg.release();

        // Ring buffer should return two separate segments instead of one due to an overflow.
        rsegs = buf.poll();
        bbuf = rsegs.get(0).buffer();

        // First segment.
        assertEquals(3, bbuf.getLong());
        assertEquals(4, bbuf.remaining());

        int pos = bbuf.position();

        byte[] tmp = new byte[8];

        byte[] arr = new byte[bbuf.capacity()];

        bbuf.position(0);

        bbuf.limit(bbuf.capacity());

        bbuf.get(arr);

        System.arraycopy(arr, pos, tmp, 0, 4);

        // One more segment available.
        bbuf = rsegs.get(1).buffer();

        assertEquals(4, bbuf.remaining());

        bbuf.position(0);

        bbuf.limit(bbuf.capacity());

        bbuf.get(arr);

        System.arraycopy(arr, 0, tmp, 4, 4);

        ByteBuffer bb = ByteBuffer.wrap(tmp);

        bb.order(ByteOrder.nativeOrder());

        assertEquals(4, bb.getLong());

        rsegs.get(1).release();

        assertNull(buf.poll());
    }

    /**
     * @param mode Mode.
     */
    private void doTestNoOverflowMultiThreaded(SegmentedRingByteBuffer.BufferMode mode) throws org.apache.ignite.IgniteCheckedException, BrokenBarrierException, InterruptedException {
        int producerCnt = 16;

        final int cap = 256 * 1024;

        final SegmentedRingByteBuffer buf = new SegmentedRingByteBuffer(cap, Long.MAX_VALUE, mode);

        final AtomicBoolean stop = new AtomicBoolean(false);

        final AtomicReference<Throwable> ex = new AtomicReference<>();

        final CyclicBarrier startBarrier = new CyclicBarrier(producerCnt);

        final CyclicBarrier restartBarrier = new CyclicBarrier(producerCnt + 1);

        final AtomicLong totalWritten = new AtomicLong();

        IgniteInternalFuture<Long> fut;

        try {
            fut = GridTestUtils.runMultiThreadedAsync(() -> {
                try {
                    try {
                        startBarrier.await();
                    }
                    catch (InterruptedException | BrokenBarrierException e) {
                        e.printStackTrace();

                        fail();
                    }

                    while (!stop.get()) {

                        TestObject obj = new TestObject();

                        SegmentedRingByteBuffer.WriteSegment seg = buf.offer(obj.size());

                        ByteBuffer bbuf;

                        if (seg == null) {
                            try {
                                restartBarrier.await(getTestTimeout(), TimeUnit.MILLISECONDS);
                            } catch (InterruptedException | TimeoutException | BrokenBarrierException e) {
                                break;
                            }

                            continue;
                        }

                        bbuf = seg.buffer();

                        assertEquals(obj.size(), bbuf.remaining());

                        bbuf.putLong(obj.id);
                        bbuf.putInt(obj.len);
                        bbuf.put(obj.arr);

                        assertEquals(0, bbuf.remaining());

                        seg.release();

                        long total = totalWritten.addAndGet(obj.size());

                        assertTrue(total <= cap);
                    }
                }
                catch (Throwable th) {
                    ex.compareAndSet(null, th);
                }
            }, producerCnt, "producer-thread");

            long endTime = System.currentTimeMillis() + 60 * 1000L;

            while (System.currentTimeMillis() < endTime && ex.get() == null) {
                while (restartBarrier.getNumberWaiting() != producerCnt && ex.get() == null)
                    U.sleep(10);

                if (ex.get() != null)
                    fail("Exception in producer thread, ex=" + ex.get());

                List<SegmentedRingByteBuffer.ReadSegment> segs = buf.poll();

                if (segs != null) {
                    for (SegmentedRingByteBuffer.ReadSegment seg : segs)
                        seg.release();
                }

                totalWritten.set(0);

                restartBarrier.await();
            }
        }
        finally {
            stop.set(true);

            restartBarrier.reset();
        }

        fut.get();

        if (ex.get() != null)
            fail("Exception in producer thread, ex=" + ex.get());
    }

    /**
     * @param mode Mode.
     */
    private void doTestMultiThreaded(SegmentedRingByteBuffer.BufferMode mode) throws org.apache.ignite.IgniteCheckedException {
        int producerCnt = 16;

        final int cap = 256 * 1024;

        final SegmentedRingByteBuffer buf = new SegmentedRingByteBuffer(cap, Long.MAX_VALUE, mode);

        final AtomicBoolean stop = new AtomicBoolean(false);

        final AtomicReference<Throwable> ex = new AtomicReference<>();

        final CyclicBarrier barrier = new CyclicBarrier(producerCnt);

        IgniteInternalFuture<Long> fut;

        try {
            fut = GridTestUtils.runMultiThreadedAsync(() -> {
                try {
                    try {
                        barrier.await();
                    }
                    catch (InterruptedException | BrokenBarrierException e) {
                        e.printStackTrace();

                        fail();
                    }

                    while (!stop.get()) {
                        TestObject obj = new TestObject();

                        SegmentedRingByteBuffer.WriteSegment seg;
                        ByteBuffer bbuf;

                        for (;;) {
                            if (stop.get())
                                return;

                            seg = buf.offer(obj.size());

                            if (seg != null)
                                break;
                        }

                        try {
                            bbuf = seg.buffer();

                            assertEquals(obj.size(), bbuf.remaining());

                            bbuf.putLong(obj.id);
                            bbuf.putInt(obj.len);
                            bbuf.put(obj.arr);

                            assertEquals(0, bbuf.remaining());

                        }
                        finally {
                            seg.release();
                        }
                    }
                }
                catch (Throwable th) {
                    ex.compareAndSet(null, th);
                }
            }, producerCnt, "producer-thread");

            Random rnd = new Random();

            long endTime = System.currentTimeMillis() + 60 * 1000L;

            while (System.currentTimeMillis() < endTime && ex.get() == null) {
                try {
                    U.sleep(rnd.nextInt(100) + 1);
                }
                catch (IgniteInterruptedCheckedException e) {
                    e.printStackTrace();
                }

                List<SegmentedRingByteBuffer.ReadSegment> segs;

                if ((segs = buf.poll()) != null) {
                    for (SegmentedRingByteBuffer.ReadSegment seg : segs) {
                        assertTrue(seg.buffer().hasRemaining());

                        seg.release();
                    }
                }
            }
        }
        finally {
            stop.set(true);
        }

        fut.get();

        if (ex.get() != null)
            fail("Exception in producer thread, ex=" + ex.get());
    }

    /**
     * @param mode Mode.
     */
    private void doTestMultiThreaded2(SegmentedRingByteBuffer.BufferMode mode) throws org.apache.ignite.IgniteCheckedException {
        int producerCnt = 16;

        final int cap = 256 * 1024;

        final SegmentedRingByteBuffer buf = new SegmentedRingByteBuffer(cap, Long.MAX_VALUE, mode);

        final AtomicReference<Throwable> ex = new AtomicReference<>();

        final AtomicBoolean stop = new AtomicBoolean(false);

        final CyclicBarrier barrier = new CyclicBarrier(producerCnt);

        final Set<TestObject> items = Collections.newSetFromMap(new ConcurrentHashMap<TestObject, Boolean>());

        IgniteInternalFuture<Long> fut;

        try {
            fut = GridTestUtils.runMultiThreadedAsync(() -> {
                try {
                    try {
                        barrier.await();
                    }
                    catch (InterruptedException | BrokenBarrierException e) {
                        e.printStackTrace();

                        fail();
                    }

                    while (!stop.get()) {
                        TestObject obj = new TestObject();

                        SegmentedRingByteBuffer.WriteSegment seg;
                        ByteBuffer bbuf;

                        for (;;) {
                            if (stop.get())
                                return;

                            seg = buf.offer(obj.size());

                            if (seg != null)
                                break;
                        }

                        try {
                            bbuf = seg.buffer();

                            assertEquals(obj.size(), bbuf.remaining());

                            bbuf.putLong(obj.id);
                            bbuf.putInt(obj.len);
                            bbuf.put(obj.arr);

                            assertEquals(0, bbuf.remaining());

                            assertTrue("Ooops! The same value is already exist in Set! ", items.add(obj));
                        }
                        finally {
                            seg.release();
                        }
                    }
                }
                catch (Throwable th) {
                    ex.compareAndSet(null, th);
                }
            }, producerCnt, "producer-thread");

            Random rnd = new Random();

            long endTime = System.currentTimeMillis() + 60 * 1000L;

            while (System.currentTimeMillis() < endTime && ex.get() == null) {
                try {
                    U.sleep(rnd.nextInt(100) + 1);
                }
                catch (IgniteInterruptedCheckedException e) {
                    e.printStackTrace();
                }

                List<SegmentedRingByteBuffer.ReadSegment> segs;

                while ((segs = buf.poll()) != null) {
                    int size = 0;

                    for (SegmentedRingByteBuffer.ReadSegment seg : segs) {
                        ByteBuffer bbuf = seg.buffer();

                        assertTrue(bbuf.hasRemaining());

                        size += bbuf.remaining();
                    }

                    byte[] arr = new byte[size];

                    int idx = 0;

                    for (SegmentedRingByteBuffer.ReadSegment seg : segs) {
                        ByteBuffer bbuf = seg.buffer();

                        assertTrue(bbuf.hasRemaining());

                        int len = bbuf.remaining();

                        bbuf.get(arr, idx, len);

                        idx += len;
                    }

                    ByteBuffer bbuf = ByteBuffer.wrap(arr);

                    bbuf.order(ByteOrder.nativeOrder());

                    assertTrue(bbuf.hasRemaining());

                    while (bbuf.hasRemaining()) {
                        long id = bbuf.getLong();

                        int len = bbuf.getInt();

                        arr = new byte[len];

                        bbuf.get(arr);

                        TestObject obj = new TestObject(id, arr);

                        assertTrue(items.remove(obj));
                    }

                    for (SegmentedRingByteBuffer.ReadSegment seg : segs)
                        seg.release();
                }
            }
        }
        finally {
            stop.set(true);
        }

        fut.get();

        if (ex.get() != null)
            fail("Exception in producer thread, ex=" + ex.get());

        List<SegmentedRingByteBuffer.ReadSegment> segs;

        while ((segs = buf.poll()) != null) {
            int size = 0;

            for (SegmentedRingByteBuffer.ReadSegment seg : segs) {
                ByteBuffer bbuf = seg.buffer();

                assertTrue(bbuf.hasRemaining());

                size += bbuf.remaining();
            }

            byte[] arr = new byte[size];

            int idx = 0;

            for (SegmentedRingByteBuffer.ReadSegment seg : segs) {
                ByteBuffer bbuf = seg.buffer();

                assertTrue(bbuf.hasRemaining());

                int len = bbuf.remaining();

                bbuf.get(arr, idx, len);

                idx += len;
            }

            ByteBuffer bbuf = ByteBuffer.wrap(arr);

            bbuf.order(ByteOrder.nativeOrder());

            assertTrue(bbuf.hasRemaining());

            while (bbuf.hasRemaining()) {
                long id = bbuf.getLong();

                int len = bbuf.getInt();

                arr = new byte[len];

                bbuf.get(arr);

                TestObject obj = new TestObject(id, arr);

                assertTrue(items.remove(obj));
            }

            for (SegmentedRingByteBuffer.ReadSegment seg : segs)
                seg.release();
        }

        assertNull(buf.poll());
        assertEquals(0, items.size());
    }

    /**
     *
     */
    private static class TestObject {
        /** Id. */
        private long id;

        /** Length. */
        private int len;

        /** Array. */
        private byte[] arr;

        /**
         * Default constructor.
         */
        public TestObject() {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            id = rnd.nextLong();
            len = rnd.nextInt(32 * 1024);
            arr = new byte[len];

            rnd.nextBytes(arr);
        }

        /**
         * @param id Id.
         * @param arr Array.
         */
        public TestObject(long id, byte[] arr) {
            this.id = id;
            this.arr = arr;

            len = arr.length;
        }

        /**
         *
         */
        public int size() {
            return 8 + 4 + arr.length;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object that) {
            if (this == that)
                return true;

            if (that == null || getClass() != that.getClass())
                return false;

            TestObject obj = (TestObject)that;

            if (id != obj.id)
                return false;

            if (len != obj.len)
                return false;

            return Arrays.equals(arr, obj.arr);

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = (int)(id ^ (id >>> 32));

            res = 31 * res + len;

            res = 31 * res + Arrays.hashCode(arr);

            return res;
        }
    }
}
