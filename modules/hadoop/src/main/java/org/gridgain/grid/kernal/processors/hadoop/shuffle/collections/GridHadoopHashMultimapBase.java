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
import org.gridgain.grid.util.offheap.unsafe.*;

import java.util.*;

/**
 * Base class for hash multimaps.
 */
public abstract class GridHadoopHashMultimapBase extends GridHadoopMultimapBase {
    /**
     * @param job Job.
     * @param mem Memory.
     */
    public GridHadoopHashMultimapBase(GridHadoopJob job, GridUnsafeMemory mem) {
        super(job, mem);
    }

    /** {@inheritDoc} */
    @Override public boolean visit(boolean ignoreLastVisited, Visitor v) throws GridException {
        throw new UnsupportedOperationException("visit");
    }

    /** {@inheritDoc} */
    @Override public GridHadoopTaskInput input() throws GridException {
        return new Input();
    }

    /**
     * @return Hash table capacity.
     */
    public abstract int capacity();

    /**
     * @param idx Index in hash table.
     * @return Meta page pointer.
     */
    protected abstract long meta(int idx);

    /** {@inheritDoc} */
    @Override public void close() {
        int metaSize = metaSize();

        for (int i = 0, cap = capacity(); i < cap; i++) {
            long meta = meta(i);

            while (meta != 0) {
                mem.release(key(meta), keySize(meta));

                long valPtr = value(meta);

                do {
                    long valPtr0 = valPtr;
                    int valSize = valueSize(valPtr) + 12;

                    valPtr = nextValue(valPtr);

                    mem.release(valPtr0, valSize);
                }
                while (valPtr != 0);

                long meta0 = meta;

                meta = collision(meta);

                mem.release(meta0, metaSize);
            }
        }
    }

    /**
     * @param keyHash Key hash.
     * @param keySize Key size.
     * @param keyPtr Key pointer.
     * @param valPtr Value page pointer.
     * @param collisionPtr Pointer to meta with hash collision.
     * @return Created meta page pointer.
     */
    protected long createMeta0(int keyHash, int keySize, long keyPtr, long valPtr, long collisionPtr) {
        int size = metaSize();

        assert size >= 32 : size;

        long meta = mem.allocate(size);

        mem.writeInt(meta, keyHash);
        mem.writeInt(meta + 4, keySize);
        mem.writeLong(meta + 8, keyPtr);
        mem.writeLong(meta + 16, valPtr);
        mem.writeLong(meta + 24, collisionPtr);

        return meta;
    }

    /**
     * @return Size of meta page.
     */
    protected int metaSize() {
        return 32;
    }

    /**
     * @param meta Meta pointer.
     * @return Key hash.
     */
    protected int keyHash(long meta) {
        return mem.readInt(meta);
    }

    /**
     * @param meta Meta pointer.
     * @return Key size.
     */
    protected int keySize(long meta) {
        return mem.readInt(meta + 4);
    }

    /**
     * @param meta Meta pointer.
     * @return Key pointer.
     */
    protected long key(long meta) {
        return mem.readLong(meta + 8);
    }

    /**
     * @param meta Meta pointer.
     * @return Value pointer.
     */
    protected long value(long meta) {
        return mem.readLong(meta + 16);
    }
    /**
     * @param meta Meta pointer.
     * @param val Value pointer.
     */
    protected void value(long meta, long val) {
        mem.writeLong(meta + 16, val);
    }

    /**
     * @param meta Meta pointer.
     * @return Collision pointer.
     */
    protected long collision(long meta) {
        return mem.readLong(meta + 24);
    }

    /**
     * @param meta Meta pointer.
     * @param collision Collision pointer.
     */
    protected void collision(long meta, long collision) {
        assert meta != collision : meta;

        mem.writeLong(meta + 24, collision);
    }


    /**
     * Reader for key and value.
     */
    protected class Reader extends ReaderBase {
        /**
         * @param ser Serialization.
         */
        protected Reader(GridHadoopSerialization ser) {
            super(ser);
        }

        /**
         * @param meta Meta pointer.
         * @return Key.
         */
        public Object readKey(long meta) {
            assert meta > 0 : meta;

            try {
                return read(key(meta), keySize(meta));
            }
            catch (GridException e) {
                throw new GridRuntimeException(e);
            }
        }
    }

    /**
     * Task input.
     */
    protected class Input implements GridHadoopTaskInput {
        /** */
        private int idx = -1;

        /** */
        private long metaPtr;

        /** */
        private final int cap;

        /** */
        private Reader keyReader;

        /** */
        private Reader valReader;

        /**
         * @throws GridException If failed.
         */
        public Input() throws GridException {
            this.cap = capacity();

            keyReader = new Reader(job.keySerialization());
            valReader = new Reader(job.valueSerialization());
        }

        /** {@inheritDoc} */
        @Override public boolean next() {
            if (metaPtr != 0) {
                metaPtr = collision(metaPtr);

                if (metaPtr != 0)
                    return true;
            }

            while (++idx < cap) { // Scan table.
                metaPtr = meta(idx);

                if (metaPtr != 0)
                    return true;
            }

            return false;
        }

        /** {@inheritDoc} */
        @Override public Object key() {
            return keyReader.readKey(metaPtr);
        }

        /** {@inheritDoc} */
        @Override public Iterator<?> values() {
            return new Iterator<Object>() {
                /** */
                private long valPtr = value(metaPtr);

                @Override public boolean hasNext() {
                    return valPtr != 0;
                }

                @Override public Object next() {
                    Object res = valReader.readValue(valPtr);

                    valPtr = nextValue(valPtr);

                    return res;
                }

                @Override public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }

        /** {@inheritDoc} */
        @Override public void close() {
            // No-op.
        }
    }
}
