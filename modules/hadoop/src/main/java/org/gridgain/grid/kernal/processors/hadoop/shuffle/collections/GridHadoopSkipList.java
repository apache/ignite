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
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.offheap.unsafe.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * Skip list.
 */
public class GridHadoopSkipList extends GridHadoopMultimapBase {
    /** */
    private final Comparator cmp;

    /** Top level. */
    private final AtomicInteger topLevel = new AtomicInteger();

    /** Heads for all the lists. */
    private final AtomicLongArray heads = new AtomicLongArray(32);

    public GridHadoopSkipList(GridHadoopJob job, GridUnsafeMemory mem) {
        super(job, mem);

        cmp = job.keyComparator();
    }

    /** {@inheritDoc} */
    @Override public boolean visit(boolean ignoreLastVisited, Visitor v) throws GridException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public Adder startAdding() throws GridException {
        return new AdderImpl();
    }

    /** {@inheritDoc} */
    @Override public GridHadoopTaskInput input() throws GridException {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // TODO
    }

    private long key(long meta) {
        return mem.readLong(meta);
    }

    private void key(long meta, long key) {
        mem.writeLong(meta, key);
    }

    private long value(long meta) {
        return mem.readLongVolatile(meta + 8);
    }

    private void value(long meta, long valPtr) {
        mem.writeLongVolatile(meta + 8, valPtr);
    }

    private boolean casValue(long meta, long oldValPtr, long newValPtr) {
        return mem.casLong(meta + 8, oldValPtr, newValPtr);
    }

    private long nextMeta(long meta, int level) {
        if (meta == 0)
            return heads.get(level);

        return mem.readLongVolatile(meta + 16 + 8 * level);
    }

    private boolean casNextMeta(long meta, int level, long oldNext, long newNext) {
        if (meta == 0)
            return heads.compareAndSet(level, oldNext, newNext);

        return mem.casLong(meta + 16 + 8 * level, oldNext, newNext);
    }

    private int keySize(long keyPtr) {
        return mem.readInt(keyPtr);
    }

    private void keySize(long keyPtr, int keySize) {
        mem.writeInt(keyPtr, keySize);
    }

    private long createMeta(long key, long val, int level) {
        long meta = mem.allocate(24 + 8 * level, true, false);

        key(meta, key);
        value(meta, val);

        return meta;
    }

    /**
     * @param rnd Random.
     * @return Next level.
     */
    static int nextLevel(Random rnd) {
        int x = rnd.nextInt();

        int level = 0;

        while ((x & 1) != 0) { // Count sequential 1 bits.
            level++;

            x >>>= 1;
        }

        return level;
    }

    private class Reader extends ReaderBase {
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
            return readValue(key(meta));
        }
    }

    private class AdderImpl extends GridHadoopMultimapBase.AdderBase {
        /** */
        private Random rnd = new GridRandom();

        /** */
        private GridLongList stack = new GridLongList(16);

        /** */
        private final Reader keyReader;

        /**
         * @throws GridException If failed.
         */
        protected AdderImpl() throws GridException {
            keyReader = new Reader(keySer);
        }

        /** {@inheritDoc} */
        @Override public void add(Object key, Object val) throws GridException {
            // TODO
        }

        /** {@inheritDoc} */
        @Override public Key addKey(DataInput in, @Nullable Key reuse) throws GridException {
            // TODO
            return null;
        }

        /**
         * @param key Key.
         * @return Pointer.
         * @throws GridException If failed.
         */
        private long writeKey(Object key) throws GridException {
            write(key, keySer);

            int keySize = out.offset();

            long keyPtr = copy(4);

            keySize(keyPtr, keySize);

            return keyPtr;
        }

        private long doAdd(Object key, @Nullable Object val) throws GridException {
            long valPtr = 0;
            long keyPtr = 0;

            if (val != null) {
                write(val, valSer);

                int valSize = out.offset();

                valPtr = copy(12);

                valueSize(valPtr, valSize);
            }

            long newMeta = 0;
            int newMetaLevel = -1;

            final int top = topLevel.get();
            long meta = heads.get(top);

            if (meta == 0) { // First entry.
                assert top == 0 : top;

                keyPtr = writeKey(key);

                newMetaLevel = 0;
                newMeta = createMeta(keyPtr, valPtr, newMetaLevel);

                if (heads.compareAndSet(0, 0, newMeta))
                    return newMeta;
            }

            for (int level = top;;) {
                Object k = keyReader.readKey(meta);

                int res = cmp.compare(key, k);

                if (res == 0) { // Key found.
                    if (newMeta != 0) { // Deallocate.
                        mem.release(keyPtr, keySize(keyPtr));
                        mem.release(newMeta, 24 + newMetaLevel);
                    }

                    if (valPtr == 0) // Only key.
                        return meta;

                    for(;;) { // Add value for the key found.
                        long nextVal = value(meta);

                        nextValue(valPtr, nextVal);

                        if (casValue(meta, nextVal, valPtr))
                            return meta;
                    }
                }

                if (res > 0)  // Go right.
                    meta = nextMeta(meta, level);
                else if (--level < 0) { // Going down.
                    // No such key.

                    // TODO
                }

                meta = nextMeta(meta, level); // Take next
            }
        }

        private void linkUp(long meta, int level) {
            for (int i = 1; i < level; i++) {


            }
        }

        /** {@inheritDoc} */
        @Override public void close() throws GridException {
            // TODO
        }
    }
}
