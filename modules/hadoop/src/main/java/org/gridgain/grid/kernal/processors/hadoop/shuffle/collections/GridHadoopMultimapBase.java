/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.shuffle.collections;

import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.shuffle.streams.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.offheap.unsafe.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.hadoop.GridHadoopJobProperty.*;

/**
 * Base class for all multimaps.
 */
public abstract class GridHadoopMultimapBase implements GridHadoopMultimap {
    /** */
    protected final GridUnsafeMemory mem;

    /** */
    protected final int pageSize;

    /** */
    private final Collection<GridLongList> allPages = new ConcurrentLinkedQueue<>();

    /**
     * @param jobInfo Job info.
     * @param mem Memory.
     */
    protected GridHadoopMultimapBase(GridHadoopJobInfo jobInfo, GridUnsafeMemory mem) {
        assert jobInfo != null;
        assert mem != null;

        this.mem = mem;

        pageSize = get(jobInfo, SHUFFLE_OFFHEAP_PAGE_SIZE, 16 * 1024);
    }

    /**
     * @param ptrs Page pointers.
     */
    private void deallocate(GridLongList ptrs) {
        while (!ptrs.isEmpty())
            mem.release(ptrs.remove(), ptrs.remove());
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
        for (GridLongList list : allPages)
            deallocate(list);
    }

    /**
     * Reader for key and value.
     */
    protected class ReaderBase implements AutoCloseable {
        /** */
        private Object tmp;

        /** */
        private final GridHadoopSerialization ser;

        /** */
        private final GridHadoopDataInStream in = new GridHadoopDataInStream(mem);

        /**
         * @param ser Serialization.
         */
        protected ReaderBase(GridHadoopSerialization ser) {
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
            catch (GridException e) {
                throw new GridRuntimeException(e);
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
        protected Object read(long ptr, long size) throws GridException {
            in.buffer().set(ptr, size);

            tmp = ser.read(in, tmp);

            return tmp;
        }

        /** {@inheritDoc} */
        @Override public void close() throws GridException {
            ser.close();
        }
    }

    /**
     * Base class for adders.
     */
    protected abstract class AdderBase implements Adder {
        /** */
        protected final GridHadoopSerialization keySer;

        /** */
        protected final GridHadoopSerialization valSer;

        /** */
        private final GridHadoopDataOutStream out;

        /** */
        private long writeStart;

        /** Size and pointer pairs list. */
        private final GridLongList pages = new GridLongList(16);

        /**
         * @param ctx Task context.
         * @throws GridException If failed.
         */
        protected AdderBase(GridHadoopTaskContext ctx) throws GridException {
            valSer = ctx.valueSerialization();
            keySer = ctx.keySerialization();

            out = new GridHadoopDataOutStream(mem) {
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

            long newPageSize = Math.max(writtenSize + requestedSize, pageSize);
            long newPagePtr = mem.allocate(newPageSize);

            pages.add(newPageSize);
            pages.add(newPagePtr);

            GridHadoopOffheapBuffer b = out.buffer();

            b.set(newPagePtr, newPageSize);

            if (writtenSize != 0) {
                mem.copyMemory(writeStart, newPagePtr, writtenSize);

                b.move(writtenSize);
            }

            writeStart = newPagePtr;

            return b.move(requestedSize);
        }

        /**
         * @return Fixed pointer.
         */
        private long fixAlignment() {
            GridHadoopOffheapBuffer b = out.buffer();

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
         * @throws GridException If failed.
         */
        protected long write(int off, Object o, GridHadoopSerialization ser) throws GridException {
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
            GridHadoopOffheapBuffer b = out.buffer();

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
        @Override public Key addKey(DataInput in, @Nullable Key reuse) throws GridException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public void close() throws GridException {
            allPages.add(pages);

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
}
