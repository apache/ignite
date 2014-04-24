/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.shuffle;

import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.util.io.*;
import org.gridgain.grid.util.offheap.unsafe.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.util.offheap.unsafe.GridUnsafeMemory.*;

/**
 * Multimap for map reduce intermediate results.
 */
public class GridHadoopMultimap implements AutoCloseable {
    /** */
    private volatile AtomicLongArray tbl;

    /** */
    private final GridHadoopJob job;

    /** */
    private final GridUnsafeMemory mem;

    /**
     * @param job Job.
     * @param mem Memory.
     */
    public GridHadoopMultimap(GridHadoopJob job, GridUnsafeMemory mem, int tblCap) {
        assert U.isPow2(tblCap);

        this.job = job;
        this.mem = mem;

        tbl = new AtomicLongArray(tblCap);
    }

    /**
     * @return Adder object.
     */
    public Adder startAdding() throws GridException {
        return new Adder();
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        for (int i = 0; i < tbl.length(); i++)  {
            long meta = tbl.get(i);

            while (meta != 0) {
                mem.release(key(meta), keySize(meta));

                long valPtr = value(meta);

                do {
                    long valPtr0 = valPtr;
                    int valSize = valueSize(valPtr);

                    valPtr = nextValue(valPtr);

                    mem.release(valPtr0, valSize);
                }
                while (valPtr != 0);

                long meta0 = meta;

                meta = collision(meta);

                mem.release(meta0, 40);
            }
        }
    }

    /**
     * @return New input for task.
     */
    public GridHadoopTaskInput input() throws GridException {
        final Reader keyReader = new Reader(job.keySerialization());
        final Reader valReader = new Reader(job.valueSerialization());

        return new GridHadoopTaskInput() {
            /** */
            private int addr = -1;

            /** */
            private long metaPtr;

            @Override public boolean next() {
                if (metaPtr != 0) {
                    metaPtr = collision(metaPtr);

                    if (metaPtr != 0)
                        return true;
                }

                while (++addr < tbl.length()) { // Scan table.
                    metaPtr = tbl.get(addr);

                    if (metaPtr != 0)
                        return true;
                }

                return false;
            }

            @Override public Object key() {
                return keyReader.readKey(metaPtr);
            }

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

            @Override public void close() throws Exception {
                // No-op.
            }
        };
    }

    /**
     * @param keyHash Key hash.
     * @param keySize Key size.
     * @param keyPtr Key pointer.
     * @param valPtr Value page pointer.
     * @param collisionPtr Pointer to meta with hash collision.
     * @param lastSentVal Last sent value pointer.
     * @return Created meta page pointer.
     */
    private long createMeta(int keyHash, int keySize, long keyPtr, long valPtr, long collisionPtr, long lastSentVal) {
        long meta = mem.allocate(40);

        mem.writeInt(meta, keyHash);
        mem.writeInt(meta + 4, keySize);
        mem.writeLong(meta + 8, keyPtr);
        mem.writeLong(meta + 16, valPtr);
        mem.writeLong(meta + 24, collisionPtr);
        mem.writeLong(meta + 32, lastSentVal);

        return meta;
    }

    /**
     * @param meta Meta pointer.
     * @return Key hash.
     */
    private int keyHash(long meta) {
        return mem.readInt(meta);
    }

    /**
     * @param meta Meta pointer.
     * @return Key size.
     */
    private int keySize(long meta) {
        return mem.readInt(meta + 4);
    }

    /**
     * @param meta Meta pointer.
     * @return Key pointer.
     */
    private long key(long meta) {
        return mem.readLong(meta + 8);
    }

    /**
     * @param meta Meta pointer.
     * @return Value pointer.
     */
    private long value(long meta) {
        return mem.readLong(meta + 16);
    }

    /**
     * @param meta Meta pointer.
     * @param oldValPtr Old value.
     * @param newValPtr New value.
     * @return {@code true} If succeeded.
     */
    private boolean casValue(long meta, long oldValPtr, long newValPtr) {
        return mem.casLong(meta + 16, oldValPtr, newValPtr);
    }

    /**
     * @param meta Meta pointer.
     * @return Collision pointer.
     */
    private long collision(long meta) {
        return mem.readLong(meta + 24);
    }

    /**
     * @param meta Meta pointer.
     * @return Last sent value pointer.
     */
    private long lastSentValue(long meta) {
        return mem.readLong(meta + 32);
    }

    /**
     * @param valPtr Value page pointer.
     * @param nextValPtr Next value page pointer.
     */
    private void nextValue(long valPtr, long nextValPtr) {
        mem.writeLong(valPtr, nextValPtr);
    }

    private long nextValue(long valPtr) {
        return mem.readLong(valPtr);
    }

    /**
     * @param valPtr Value page pointer.
     * @param size Size.
     */
    private void valueSize(long valPtr, int size) {
        mem.writeInt(valPtr + 8, size);
    }

    /**
     * @param valPtr Value page pointer.
     * @return Value size.
     */
    private int valueSize(long valPtr) {
        return mem.readInt(valPtr + 8);
    }

    /**
     * Reader for key and value.
     */
    private class Reader {
        /** */
        private Object tmp;

        /** */
        private final GridHadoopSerialization ser;

        /** */
        private final GridHadoopDataInStream in = new GridHadoopDataInStream();

        /**
         * @param ser Serialization.
         */
        private Reader(GridHadoopSerialization ser) {
            this.ser = ser;
        }

        /**
         * @param meta Meta pointer.
         * @return Key.
         */
        public Object readKey(long meta) {
            assert meta > 0 : meta;

            return read(key(meta), keySize(meta));
        }

        /**
         * @param valPtr Value page pointer.
         * @return Value.
         */
        public Object readValue(long valPtr) {
            assert valPtr > 0 : valPtr;

            return read(valPtr + 12, valueSize(valPtr));
        }

        /**
         * @param ptr Pointer.
         * @param size Object size.
         * @return Object.
         */
        private Object read(long ptr, long size) {
            in.buffer().buffer(ptr, size);

            try {
                tmp = ser.read(in, tmp);
            }
            catch (IOException e) {
                throw new IllegalStateException(e);
            }

            return tmp;
        }
    }

    /**
     * Adder.
     */
    public class Adder implements AutoCloseable {
        /** */
        private final GridHadoopSerialization keySer;

        /** */
        private final GridHadoopSerialization valSer;

        /** */
        private final GridUnsafeDataOutput out = new GridUnsafeDataOutput(256);

        /** */
        private final Reader keyReader;

        public Adder() throws GridException {
            valSer = job.valueSerialization();
            keySer = job.keySerialization();
            keyReader = new Reader(keySer);
        }

        /**
         * Adds value for the given key.
         *
         * @param key Key.
         * @param val Value.
         * @return New meta page pointer if new key was inserted or {@code 0} otherwise.
         */
        public long add(Object key, Object val) {
            int keyHash = U.hash(key.hashCode());

            AtomicLongArray tbl0 = tbl;

            int addr = keyHash & (tbl0.length() - 1);

            long newMetaPtr = 0;

            write(val, valSer);

            int valSize = out.offset();

            long valPtr = copy(12);

            valueSize(valPtr, valSize);

            for (;;) {
                long metaPtr0 = tbl0.get(addr); // Read root meta pointer at this address.

                if (metaPtr0 != 0) {
                    long metaPtr = metaPtr0;

                    do { // Scan all the collisions.
                        if (keyHash(metaPtr) == keyHash && key.equals(keyReader.readKey(metaPtr))) { // Found key.
                            if (newMetaPtr != 0) { // Deallocate new meta if one was allocated.
                                mem.release(key(newMetaPtr), keySize(newMetaPtr));
                                mem.release(newMetaPtr, 40);
                            }

                            long nextValPtr;

                            // Values are linked to each other to a stack like structure.
                            // Replace the last value in meta with ours and link it as next.
                            do {
                                nextValPtr = value(metaPtr);

                                nextValue(valPtr, nextValPtr);
                            }
                            while (!casValue(metaPtr, nextValPtr, valPtr));

                            return 0; // New key was not added.
                        }

                        metaPtr = collision(metaPtr);
                    }
                    while (metaPtr != 0);
                }

                if (newMetaPtr == 0) {
                    write(key, keySer);

                    int keySize = out.offset();

                    long keyPtr = copy(0);

                    nextValue(valPtr, 0);

                    newMetaPtr = createMeta(keyHash, keySize, keyPtr, valPtr, 0, 0);
                }

                if (tbl0.compareAndSet(addr, metaPtr0, newMetaPtr)) // Try to replace root meta pointer with new one.
                    return newMetaPtr;
            }
        }

        /**
         * @param o Object.
         */
        private void write(Object o, GridHadoopSerialization ser) {
            out.reset();

            try {
                ser.write(out, o);
            }
            catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        private long copy(int off) {
            int size = out.offset();

            long ptr = mem.allocate(off + size);

            UNSAFE.copyMemory(out.internalArray(), BYTE_ARR_OFF, null, ptr + off, size);

            return ptr;
        }

        @Override public void close()  {
            // TODO
        }
    }
}
