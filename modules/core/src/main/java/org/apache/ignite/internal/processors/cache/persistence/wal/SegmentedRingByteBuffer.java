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
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import org.apache.ignite.internal.processors.cache.persistence.DataStorageMetricsImpl;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.internal.S;

import static java.nio.ByteBuffer.allocate;
import static java.nio.ByteBuffer.allocateDirect;
import static org.apache.ignite.internal.processors.cache.persistence.wal.SegmentedRingByteBuffer.BufferMode.DIRECT;
import static org.apache.ignite.internal.processors.cache.persistence.wal.SegmentedRingByteBuffer.BufferMode.MAPPED;

/**
 * Segmented ring byte buffer that represents multi producer/single consumer queue that can be used by multiple writer
 * threads and one reader thread.
 */
public class SegmentedRingByteBuffer {
    /** Open mask. */
    private static final long OPEN_MASK = 0x7FFFFFFFFFFFFFFFL;

    /** Close mask. */
    private static final long CLOSE_MASK = 0x8000000000000000L;

    /** Tail field atomic updater. */
    private static final AtomicLongFieldUpdater<SegmentedRingByteBuffer> TAIL_UPD =
        AtomicLongFieldUpdater.newUpdater(SegmentedRingByteBuffer.class, "tail");

    /** Producers count field atomic updater. */
    private static final AtomicIntegerFieldUpdater<SegmentedRingByteBuffer> PRODUCERS_CNT_UPD =
        AtomicIntegerFieldUpdater.newUpdater(SegmentedRingByteBuffer.class, "producersCnt");

    /** Capacity. */
    private final int cap;

    /** Direct. */
    private final BufferMode mode;

    /** Buffer. */
    public final ByteBuffer buf;

    /** Max segment size. */
    private final long maxSegmentSize;

    /** Head. */
    private volatile long head;

    /** Tail. */
    private volatile long tail;

    /**
     * Producers count. Uses by consumer in order to wait for ending of data writing by all producers.
     */
    private volatile int producersCnt;

    /**
     * Wait for consumer flag. Prevents producers from writing data to the ring buffer while consumer waiting for finish
     * of all already writing producers.
     */
    private volatile boolean waitForConsumer;

    /** Metrics. */
    private final DataStorageMetricsImpl metrics;

    /**
     * Creates ring buffer with given capacity.
     *
     * @param cap Buffer's capacity.
     * @param maxSegmentSize Max segment size.
     * @param mode Buffer mode.
     */
    public SegmentedRingByteBuffer(int cap, long maxSegmentSize, BufferMode mode) {
        this(cap, maxSegmentSize, mode == DIRECT ? allocateDirect(cap) : allocate(cap), mode, null);
    }

    /**
     * Creates ring buffer with given capacity.
     *
     * @param cap Buffer's capacity.
     * @param maxSegmentSize Max segment size.
     * @param mode Buffer mode.
     * @param metrics Metrics.
     */
    public SegmentedRingByteBuffer(int cap, long maxSegmentSize, BufferMode mode, DataStorageMetricsImpl metrics) {
        this(cap, maxSegmentSize, mode == DIRECT ? allocateDirect(cap) : allocate(cap), mode, metrics);
    }

    /**
     * Creates ring buffer with given capacity which mapped to file.
     *
     * @param buf {@link MappedByteBuffer} instance.
     * @param metrics Metrics.
     */
    public SegmentedRingByteBuffer(MappedByteBuffer buf, DataStorageMetricsImpl metrics) {
        this(buf.capacity(), buf.capacity(), buf, MAPPED, metrics);
    }

    /**
     * @param cap Capacity.
     * @param maxSegmentSize Max segment size.
     * @param buf Buffer.
     * @param mode Mode.
     * @param metrics Metrics.
     */
    private SegmentedRingByteBuffer(
        int cap,
        long maxSegmentSize,
        ByteBuffer buf,
        BufferMode mode,
        DataStorageMetricsImpl metrics
    ) {
        this.cap = cap;
        this.mode = mode;
        this.buf = buf;
        this.buf.order(ByteOrder.nativeOrder());
        this.maxSegmentSize = maxSegmentSize;
        this.metrics = metrics;
    }

    /**
     * Performs initialization of ring buffer state.
     *
     * @param pos Position.
     */
    public void init(long pos) {
        head = pos;
        tail = pos;
    }

    /**
     * Returns buffer mode.
     *
     * @return Buffer mode.
     */
    public BufferMode mode() {
        return mode;
    }

    /**
     * Returns actual buffer tail.
     *
     * @return Buffer tail.
     */
    public long tail() {
        return tail & SegmentedRingByteBuffer.OPEN_MASK;
    }

    /**
     * Reserves {@code size} bytes in {@code SegmentedRingByteBuffer} and returns instance of {@link WriteSegment}
     * class that points to wrapped {@link ByteBuffer} instance with corresponding capacity. This {@link ByteBuffer}
     * instance should be used only for data writing to {@link SegmentedRingByteBuffer}.
     * <p>
     * Returned result can be {@code null} in case of requested amount of bytes greater then available space
     * in {@code SegmentedRingByteBuffer}. Also {@link WriteSegment#buffer()} can return {@code null} in case of
     * {@link #maxSegmentSize} value is exceeded. In this case buffer will be closed in order to prevent any
     * concurrent threads from trying of reserve new segment.
     * <p>
     * This method can be invoked by many producer threads and each producer will get own {@link ByteBuffer} instance
     * that mapped to own {@link SegmentedRingByteBuffer} slice.
     * <p>
     * Once the data has been written into the {@link ByteBuffer} client code must notify
     * {@code SegmentedRingByteBuffer} instance using {@link WriteSegment#release()} method in order to provide
     * possibility to consumer get data for reading.
     *
     * @param size Amount of bytes for reserve.
     * @return {@link WriteSegment} instance that point to {@link ByteBuffer} instance with given {@code size}.
     * {@code null} if buffer space is not enough.
     */
    public WriteSegment offer(int size) {
        return offer0(size, false);
    }

    /**
     * Behaves like {@link #offer(int)} but in safe manner: there are no any concurrent threads and buffer in
     * closed state.
     *
     * @param size Amount of bytes for reserve.
     * @return {@link WriteSegment} instance that point to {@link ByteBuffer} instance with given {@code size}.
     * {@code null} if buffer space is not enough.
     */
    public WriteSegment offerSafe(int size) {
        return offer0(size, true);
    }

    /**
     * @param size Amount of bytes for reserve.
     * @param safe Safe mode.
     */
    private WriteSegment offer0(int size, boolean safe) {
        if (size > cap)
            throw new IllegalArgumentException("Record is too long [capacity=" + cap + ", size=" + size + ']');

        for (;;) {
            if (!waitForConsumer) {
                int cur = producersCnt;

                if (cur >= 0 && PRODUCERS_CNT_UPD.compareAndSet(this, cur, cur + 1))
                    break;
            }
        }

        for (;;) {
            long currTail = tail;

            assert !safe || currTail < 0 : "Unsafe usage of segment ring byte buffer currTail=" + currTail;

            if (currTail < 0) {
                if (safe)
                    currTail &= SegmentedRingByteBuffer.OPEN_MASK;
                else
                    return new WriteSegment(null, -1);
            }

            long head0 = head;

            long currTailIdx = toIndex(currTail);

            boolean fitsSeg = currTail + size <= maxSegmentSize;

            long newTail = fitsSeg ? currTail + size : currTail;

            if (head0 < newTail - cap) { // Not enough space.
                PRODUCERS_CNT_UPD.decrementAndGet(this);

                return null;
            }
            else {
                // If safe we should keep buffer closed.
                long tail0 = fitsSeg ? (safe ? newTail | CLOSE_MASK : newTail) : newTail | CLOSE_MASK;

                boolean upd = TAIL_UPD.compareAndSet(this, safe ? currTail | CLOSE_MASK : currTail, tail0);

                assert !safe || upd : "Unsafe usage of segment ring byte buffer";

                if (upd) {
                    if (!fitsSeg)
                        return new WriteSegment(null, -1);

                    boolean wrap = cap - currTailIdx < size;

                    if (wrap) {
                        long newTailIdx = toIndex(newTail);

                        return new WriteSegment(currTail, newTail, newTailIdx == 0 ? newTail : currTail);
                    }
                    else {
                        ByteBuffer slice = slice((int)toIndex(newTail - size), size, false);

                        return new WriteSegment(slice, newTail);
                    }
                }
            }
        }
    }

    /**
     * Closes the buffer.
     */
    public void close() {
        for (;;) {
            long currTail = tail;

            if (currTail < 0)
                return;

            if(TAIL_UPD.compareAndSet(this, currTail, currTail | CLOSE_MASK))
                return;
        }
    }

    /**
     * Retrieves list of {@link ReadSegment} instances that point to {@link ByteBuffer} that contains all data available
     * for reading from {@link SegmentedRingByteBuffer} or {@code null} if there are no available data for reading.
     * <p>
     * This method can be invoked only by one consumer thread.
     * <p>
     * Once the data has been read from the returned {@link ReadSegment} client code must notify
     * {@link SegmentedRingByteBuffer} instance using {@link ReadSegment#release()} method in order to release occupied
     * space in the {@link SegmentedRingByteBuffer} and make it available for writing.
     *
     * @return List of {@code ReadSegment} instances with all available data for reading or {@code null} if
     * there are no available data.
     */
    public List<ReadSegment> poll() {
        return poll(-1);
    }

    /**
     * Retrieves list of {@link ReadSegment} instances that point to {@link ByteBuffer} that contains data
     * available for reading from {@link SegmentedRingByteBuffer} limited by {@code pos} parameter or {@code null}
     * if there are no available data for reading.
     * <p>
     * This method can be invoked only by one consumer thread.
     * <p>
     * Once the data has been read from the returned {@link ReadSegment} client code must notify
     * {@link SegmentedRingByteBuffer} instance using {@link ReadSegment#release()} method in order to release occupied
     * space in the {@link SegmentedRingByteBuffer} and make it available for writing.
     *
     * @param pos End position in buffer.
     * @return List of {@code ReadSegment} instances with all available data for reading or {@code null} if
     * there are no available data.
     */
    public List<ReadSegment> poll(long pos) {
        waitForConsumer = true;

        int spins = 0;

        for (;;) {
            if (PRODUCERS_CNT_UPD.compareAndSet(this, 0, -1))
                break;

            spins++;
        }

        if (metrics != null && metrics.metricsEnabled())
            metrics.onBuffPollSpin(spins);

        long head = this.head;

        long tail = this.tail & OPEN_MASK;

        producersCnt = 0;

        waitForConsumer = false;

        // There are no data for reading or all data up to given position were read.
        if (tail <= head || (pos >=0 && head > pos))
            return null;

        int headIdx = (int)toIndex(head);

        int tailIdx = (int)toIndex(tail);

        boolean wrapped = tailIdx <= headIdx;

        if (wrapped && tailIdx != 0) {
            List<ReadSegment> lst = new ArrayList<>(2);

            int lim = cap - headIdx;

            lst.add(new ReadSegment(slice(headIdx, lim, true), head, head + lim));

            lst.add(new ReadSegment(slice(0, tailIdx, true), head + lim, tail));

            return lst;
        }
        else
            return Collections.singletonList(new ReadSegment(slice(headIdx, (int)(tail - head), true), head, tail));
    }

    /**
     * Frees allocated memory in case of direct byte buffer.
     */
    public void free() {
        if (mode == DIRECT || mode == MAPPED)
            GridUnsafe.cleanDirectBuffer(buf);
    }

    /**
     * Resets the state of the buffer and returns new instance but with the same underlying buffer.
     */
    public SegmentedRingByteBuffer reset() {
        return new SegmentedRingByteBuffer(buf.capacity(), maxSegmentSize, buf, mode, metrics);
    }

    /**
     * @param off Offset.
     * @param len Length.
     * @param readOnly Read only.
     */
    private ByteBuffer slice(int off, int len, boolean readOnly) {
        ByteBuffer bb = readOnly ? buf.asReadOnlyBuffer() : buf.duplicate();

        bb.order(ByteOrder.nativeOrder());
        bb.limit(off + len);
        bb.position(off);

        return bb;
    }

    /**
     * @param globalIdx Global index of ring buffer.
     * @return Index of byte array.
     */
    private long toIndex(long globalIdx) {
        return globalIdx % cap;
    }

    /**
     * @param src Source.
     * @param srcPos Source pos.
     * @param dest Destination.
     * @param destPos Destination pos.
     * @param len Length.
     */
    private void copy(ByteBuffer src, int srcPos, ByteBuffer dest, int destPos, int len) {
        assert mode != MAPPED;

        if (buf.isDirect()) {
            ByteBuffer src0 = src.duplicate();
            src0.limit(srcPos + len);
            src0.position(srcPos);

            ByteBuffer dest0 = dest.duplicate();
            dest0.limit(destPos + len);
            dest0.position(destPos);

            dest0.put(src0);
        }
        else
            System.arraycopy(src.array(), srcPos, buf.array(), destPos, len);
    }

    /**
     *
     */
    private abstract class Segment {
        /** Buffer. */
        protected final ByteBuffer seg;

        /** Pos. */
        protected final long pos;

        /**
         * @param seg Seg.
         * @param pos Pos.
         */
        protected Segment(ByteBuffer seg, long pos) {
            this.seg = seg;
            this.pos = pos;
        }

        /**
         * Releases segment.
         */
        abstract public void release();

        /**
         * Returns byte buffer.
         *
         * @return Byte buffer.
         */
        abstract public ByteBuffer buffer();

        /**
         * Returns position.
         *
         * @return Position.
         */
        public long position() {
            return pos;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Segment.class, this);
        }
    }

    /**
     * Segment available for data writing.
     */
    public class WriteSegment extends Segment {
        /** Current tail. */
        private final long currTail;

        /** Wrap point. */
        private final long wrapPnt;

        /**
         * @param currTail Current tail.
         * @param newTail New tail.
         * @param wrapPnt Wrap point.
         */
        private WriteSegment(long currTail, long newTail, long wrapPnt) {
            super(allocate((int)(newTail - currTail)), newTail);

            this.seg.order(ByteOrder.nativeOrder());
            this.currTail = currTail;
            this.wrapPnt = wrapPnt;
        }

        /**
         * @param seg Seg.
         * @param pos Pos.
         */
        private WriteSegment(ByteBuffer seg, long pos) {
            super(seg, pos);

            this.currTail = -1;
            this.wrapPnt = -1;
        }

        /** {@inheritDoc} */
        @Override public ByteBuffer buffer() {
            return seg;
        }

        /** {@inheritDoc} */
        @Override public void release() {
            if (wrapPnt > -1) {
                int pos = (int)toIndex(currTail);

                int len = cap - pos;

                copy(seg, 0, buf, pos, len);

                copy(seg, len, buf, 0, seg.array().length - len);
            }

            assert producersCnt >= 0;

            PRODUCERS_CNT_UPD.decrementAndGet(SegmentedRingByteBuffer.this);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(WriteSegment.class, this, "super", super.toString());
        }
    }

    /**
     * Segment available for data reading.
     */
    public class ReadSegment extends Segment {
        /** New head. */
        private final long newHead;

        /**
         * @param seg Seg.
         * @param pos Pos.
         * @param newHead New head.
         */
        private ReadSegment(ByteBuffer seg, long pos, long newHead) {
            super(seg, pos);

            this.newHead = newHead;
        }

        /** {@inheritDoc} */
        @Override public void release() {
            if (newHead >= 0)
                head = newHead;
        }

        /** {@inheritDoc} */
        @Override public ByteBuffer buffer() {
            return seg;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ReadSegment.class, this, "super", super.toString());
        }
    }

    /**
     * Buffer mode.
     */
    public enum BufferMode {
        /** Byte buffer on-heap. */
        ONHEAP,

        /** Direct byte buffer off-heap */
        DIRECT,

        /** Byte buffer mapped to file. */
        MAPPED
    }
}
