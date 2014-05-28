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
import org.gridgain.grid.util.io.*;
import org.gridgain.grid.util.offheap.unsafe.*;
import org.jetbrains.annotations.*;

import java.io.*;

import static org.gridgain.grid.util.offheap.unsafe.GridUnsafeMemory.*;

/**
 * Base class for all multimaps.
 */
public abstract class GridHadoopMultimapBase implements GridHadoopMultimap {
    /** */
    protected final GridHadoopJob job;

    /** */
    protected final GridUnsafeMemory mem;

    /**
     * @param job Job.
     * @param mem Memory.
     */
    public GridHadoopMultimapBase(GridHadoopJob job, GridUnsafeMemory mem) {
        assert job != null;
        assert mem != null;

        this.job = job;
        this.mem = mem;
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

    /**
     * Reader for key and value.
     */
    protected class ReaderBase {
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
         * @param ptr Pointer.
         * @param size Object size.
         * @return Object.
         */
        protected Object read(long ptr, long size) throws GridException {
            in.buffer().set(ptr, size);

            tmp = ser.read(in, tmp);

            return tmp;
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
        protected final GridUnsafeDataOutput out = new GridUnsafeDataOutput(256);

        /**
         * @throws GridException If failed.
         */
        protected AdderBase() throws GridException {
            valSer = job.valueSerialization();
            keySer = job.keySerialization();
        }

        /**
         * @param o Object.
         */
        protected void write(Object o, GridHadoopSerialization ser) throws GridException {
            out.reset();

            ser.write(out, o);
        }

        /**
         * @param off Offset.
         * @return Allocated pointer.
         */
        protected long copy(int off) {
            int size = out.offset();

            long ptr = mem.allocate(off + size);

            UNSAFE.copyMemory(out.internalArray(), BYTE_ARR_OFF, null, ptr + off, size);

            return ptr;
        }

        /** */
        @Override public Key addKey(DataInput in, @Nullable Key reuse) throws GridException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public void close() throws GridException {
            keySer.close();
            valSer.close();
        }
    }
}
