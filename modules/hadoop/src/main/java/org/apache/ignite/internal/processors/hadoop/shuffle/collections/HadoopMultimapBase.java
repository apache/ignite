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

package org.apache.ignite.internal.processors.hadoop.shuffle.collections;

import java.io.DataInput;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.hadoop.HadoopJobInfo;
import org.apache.ignite.internal.processors.hadoop.HadoopSerialization;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskContext;
import org.apache.ignite.internal.processors.hadoop.shuffle.streams.HadoopDataInStream;
import org.apache.ignite.internal.processors.hadoop.shuffle.streams.HadoopDataOutStream;
import org.apache.ignite.internal.processors.hadoop.shuffle.streams.HadoopOffheapBuffer;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.hadoop.HadoopJobProperty.SHUFFLE_OFFHEAP_PAGE_SIZE;
import static org.apache.ignite.internal.processors.hadoop.HadoopJobProperty.get;

/**
 * Base class for all multimaps.
 */
public abstract class HadoopMultimapBase implements HadoopMultimap {
    /** Default offheap page size. */
    private static final int DFLT_OFFHEAP_PAGE_SIZE = 1024 * 1024;

    /** */
    protected final GridUnsafeMemory mem;

    /** */
    protected final int pageSize;

    /** */
    private final Collection<Page> allPages = new ConcurrentLinkedQueue<>();

    /**
     * @param jobInfo Job info.
     * @param mem Memory.
     */
    protected HadoopMultimapBase(HadoopJobInfo jobInfo, GridUnsafeMemory mem) {
        assert jobInfo != null;
        assert mem != null;

        this.mem = mem;

        pageSize = get(jobInfo, SHUFFLE_OFFHEAP_PAGE_SIZE, DFLT_OFFHEAP_PAGE_SIZE);
    }

    /**
     * @param page Page.
     */
    private void deallocate(Page page) {
        assert page != null;

        mem.release(page.ptr, page.size);
    }

    /**
     * @param valPtr Value page pointer.
     * @param nextValPtr Next value page pointer.
     */
    protected void nextValue(long valPtr, long nextValPtr) {
        mem.writeLong(valPtr, nextValPtr);
    }

    /**
     * @param valPtr Value page pointer.
     * @return Next value page pointer.
     */
    protected long nextValue(long valPtr) {
        return mem.readLong(valPtr);
    }

    /**
     * @param valPtr Value page pointer.
     * @param size Size.
     */
    protected void valueSize(long valPtr, int size) {
        mem.writeInt(valPtr + 8, size);
    }

    /**
     * @param valPtr Value page pointer.
     * @return Value size.
     */
    protected int valueSize(long valPtr) {
        return mem.readInt(valPtr + 8);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        for (Page page : allPages)
            deallocate(page);
    }

    /**
     * Reader for key and value.
     */
    protected class ReaderBase implements AutoCloseable {
        /** */
        private Object tmp;

        /** */
        private final HadoopSerialization ser;

        /** */
        private final HadoopDataInStream in = new HadoopDataInStream(mem);

        /**
         * @param ser Serialization.
         */
        protected ReaderBase(HadoopSerialization ser) {
            assert ser != null;

            this.ser = ser;
        }

        /**
         * @param valPtr Value page pointer.
         * @return Value.
         */
        public Object readValue(long valPtr) {
            assert valPtr > 0 : valPtr;

            try {
                return read(valPtr + 12, valueSize(valPtr));
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /**
         * Resets temporary object to the given one.
         *
         * @param tmp Temporary object for reuse.
         */
        public void resetReusedObject(Object tmp) {
            this.tmp = tmp;
        }

        /**
         * @param ptr Pointer.
         * @param size Object size.
         * @return Object.
         */
        protected Object read(long ptr, long size) throws IgniteCheckedException {
            in.buffer().set(ptr, size);

            tmp = ser.read(in, tmp);

            return tmp;
        }

        /** {@inheritDoc} */
        @Override public void close() throws IgniteCheckedException {
            ser.close();
        }
    }

    /**
     * Base class for adders.
     */
    protected abstract class AdderBase implements Adder {
        /** */
        protected final HadoopSerialization keySer;

        /** */
        protected final HadoopSerialization valSer;

        /** */
        private final HadoopDataOutStream out;

        /** */
        private long writeStart;

        /** Current page. */
        private Page curPage;

        /**
         * @param ctx Task context.
         * @throws IgniteCheckedException If failed.
         */
        protected AdderBase(HadoopTaskContext ctx) throws IgniteCheckedException {
            valSer = ctx.valueSerialization();
            keySer = ctx.keySerialization();

            out = new HadoopDataOutStream(mem) {
                @Override public long move(long size) {
                    long ptr = super.move(size);

                    if (ptr == 0) // Was not able to move - not enough free space.
                        ptr = allocateNextPage(size);

                    assert ptr != 0;

                    return ptr;
                }
            };
        }

        /**
         * @param requestedSize Requested size.
         * @return Next write pointer.
         */
        private long allocateNextPage(long requestedSize) {
            int writtenSize = writtenSize();

            long newPageSize = nextPageSize(writtenSize + requestedSize);
            long newPagePtr = mem.allocate(newPageSize);

            HadoopOffheapBuffer b = out.buffer();

            b.set(newPagePtr, newPageSize);

            if (writtenSize != 0) {
                mem.copyMemory(writeStart, newPagePtr, writtenSize);

                b.move(writtenSize);
            }

            writeStart = newPagePtr;

            // At this point old page is not needed, so we release it.
            Page oldPage = curPage;

            curPage = new Page(newPagePtr, newPageSize);

            if (oldPage != null)
                allPages.add(oldPage);

            return b.move(requestedSize);
        }

        /**
         * Get next page size.
         *
         * @param required Required amount of data.
         * @return Next page size.
         */
        private long nextPageSize(long required) {
            long pages = (required / pageSize) + 1;

            long pagesPow2 = nextPowerOfTwo(pages);

            return pagesPow2 * pageSize;
        }

        /**
         * Get next power of two which greater or equal to the given number. Naive implementation.
         *
         * @param val Number
         * @return Nearest pow2.
         */
        private long nextPowerOfTwo(long val) {
            long res = 1;

            while (res < val)
                res = res << 1;

            if (res < 0)
                throw new IllegalArgumentException("Value is too big to find positive pow2: " + val);

            return res;
        }

        /**
         * @return Fixed pointer.
         */
        private long fixAlignment() {
            HadoopOffheapBuffer b = out.buffer();

            long ptr = b.pointer();

            if ((ptr & 7L) != 0) { // Address is not aligned by octet.
                ptr = (ptr + 8L) & ~7L;

                b.pointer(ptr);
            }

            return ptr;
        }

        /**
         * @param off Offset.
         * @param o Object.
         * @return Page pointer.
         * @throws IgniteCheckedException If failed.
         */
        protected long write(int off, Object o, HadoopSerialization ser) throws IgniteCheckedException {
            writeStart = fixAlignment();

            if (off != 0)
                out.move(off);

            ser.write(out, o);

            return writeStart;
        }

        /**
         * @param size Size.
         * @return Pointer.
         */
        protected long allocate(int size) {
            writeStart = fixAlignment();

            out.move(size);

            return writeStart;
        }

        /**
         * Rewinds local allocation pointer to the given pointer if possible.
         *
         * @param ptr Pointer.
         */
        protected void localDeallocate(long ptr) {
            HadoopOffheapBuffer b = out.buffer();

            if (b.isInside(ptr))
                b.pointer(ptr);
            else
                b.reset();
        }

        /**
         * @return Written size.
         */
        protected int writtenSize() {
            return (int)(out.buffer().pointer() - writeStart);
        }

        /** {@inheritDoc} */
        @Override public Key addKey(DataInput in, @Nullable Key reuse) throws IgniteCheckedException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public void close() throws IgniteCheckedException {
            if (curPage != null)
                allPages.add(curPage);

            keySer.close();
            valSer.close();
        }
    }

    /**
     * Iterator over values.
     */
    protected class ValueIterator implements Iterator<Object> {
        /** */
        private long valPtr;

        /** */
        private final ReaderBase valReader;

        /**
         * @param valPtr Value page pointer.
         * @param valReader Value reader.
         */
        protected ValueIterator(long valPtr, ReaderBase valReader) {
            this.valPtr = valPtr;
            this.valReader = valReader;
        }

        /**
         * @param valPtr Head value pointer.
         */
        public void head(long valPtr) {
            this.valPtr = valPtr;
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return valPtr != 0;
        }

        /** {@inheritDoc} */
        @Override public Object next() {
            if (!hasNext())
                throw new NoSuchElementException();

            Object res = valReader.readValue(valPtr);

            valPtr = nextValue(valPtr);

            return res;
        }

        /** {@inheritDoc} */
        @Override public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Page.
     */
    private static class Page {
        /** Pointer. */
        private final long ptr;

        /** Size. */
        private final long size;

        /**
         * Constructor.
         *
         * @param ptr Pointer.
         * @param size Size.
         */
        public Page(long ptr, long size) {
            this.ptr = ptr;
            this.size = size;
        }
    }
}