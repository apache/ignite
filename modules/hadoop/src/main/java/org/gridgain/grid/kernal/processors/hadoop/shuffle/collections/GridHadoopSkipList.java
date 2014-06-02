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
import org.gridgain.grid.util.typedef.internal.*;
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

    /**
     * @param rnd Random.
     * @return Next level.
     */
    static int randomLevel(Random rnd) {
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
        @Override public void write(Object key, Object val) throws GridException {
            A.notNull(val, "val");

            add(key, val);
        }

        /** {@inheritDoc} */
        @Override public Key addKey(DataInput in, @Nullable Key reuse) throws GridException {
            KeyImpl k = reuse == null ? new KeyImpl() : (KeyImpl)reuse;

            k.tmpKey = keySer.read(in, k.tmpKey);

            k.meta = add(k.tmpKey, null);

            return k;
        }

        /**
         * @param key Key.
         * @param val Value.
         * @param level Level.
         * @return Meta pointer.
         */
        private long createMeta(long key, long val, int level) {
            int size = 24 + 8 * level;

            long meta = allocate(size);

            key(meta, key);
            value(meta, val);

            for (int i = 24; i < size; i += 8) // Fill with 0.
                mem.writeLong(meta + i, 0L);

            return meta;
        }

        /**
         * @param key Key.
         * @return Pointer.
         * @throws GridException If failed.
         */
        private long writeKey(Object key) throws GridException {
            long keyPtr = write(4, key, keySer);
            int keySize = writtenSize() - 4;

            keySize(keyPtr, keySize);

            return keyPtr;
        }

        /**
         * @param key Key.
         * @param val Value.
         * @return Meta pointer.
         * @throws GridException If failed.
         */
        private long add(Object key, @Nullable Object val) throws GridException {
            long valPtr = 0;
            long keyPtr = 0;

            if (val != null) { // Write value.
                valPtr = write(12, val, valSer);
                int valSize = writtenSize() - 12;

                valueSize(valPtr, valSize);
            }

            long newMeta = 0;
            int newMetaLevel = -1;

            int top = topLevel.get();
            long prevMeta = 0;
            long meta = heads.get(top);

            for (int level = top;;) {
                while (meta == 0 && level != 0) // If we can go down here we have to.
                    meta = nextMeta(prevMeta, --level);

                if (meta == 0) { // We've found nothing, try to add new meta.
                    assert level == 0 : level;

                    if (keyPtr == 0) { // Write key and create meta.
                        keyPtr = writeKey(key);

                        newMetaLevel = randomLevel(rnd);
                        newMeta = createMeta(keyPtr, valPtr, newMetaLevel);
                    }

                    if (casNextMeta(prevMeta, 0, meta, newMeta)) { // We just added new key.
                        laceUp(newMeta, newMetaLevel);

                        return newMeta;
                    }
                    else { // Add failed, need to check out what was added by another thread.
                        meta = nextMeta(prevMeta, 0);

                        continue;
                    }
                }

                Object k = keyReader.readKey(meta);

                int res = cmp.compare(key, k);

                if (res == 0) { // Key found.
                    if (newMeta != 0)  // Deallocate.
                        localDeallocate(keyPtr);

                    if (valPtr == 0) // Only key.
                        return meta;

                    for(;;) { // Add value for the key found.
                        long nextVal = value(meta);

                        nextValue(valPtr, nextVal);

                        if (casValue(meta, nextVal, valPtr))
                            return meta;
                    }
                }

                if (res > 0) { // Go right.
                    prevMeta = meta;
                    meta = nextMeta(meta, level);
                }
                else if (level != 0)
                    level--; // Go down.
            }
        }

        /**
         * @param newMeta New meta pointer.
         * @param level Level.
         */
        private void laceUp(long newMeta, int level) {
            for (int i = 1; i < level; i++) {


            }
        }

        /**
         * Key.
         */
        public class KeyImpl implements Key {
            /** */
            private long meta;

            /** */
            private Object tmpKey;

            /**
             * @return Meta pointer for the key.
             */
            public long address() {
                return meta;
            }

            /**
             * @param val Value.
             */
            @Override public void add(Value val) {
                int size = val.size();

                long valPtr = allocate(size + 12);

                val.copyTo(valPtr + 12);

                valueSize(valPtr, size);

                long nextVal;

                do {
                    nextVal = value(meta);

                    nextValue(valPtr, nextVal);
                }
                while(!casValue(meta, nextVal, valPtr));
            }
        }
    }
}
