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
    private final AtomicInteger topLevel = new AtomicInteger(-1);

    /** Heads for all the lists. */
    private final AtomicLongArray heads = new AtomicLongArray(32);

    public GridHadoopSkipList(GridHadoopJob job, GridUnsafeMemory mem) {
        super(job, mem);

        cmp = job.keyComparator();
    }

    /** {@inheritDoc} */
    @Override public boolean visit(boolean ignoreLastVisited, Visitor v) throws GridException {
        return false; // TODO
    }

    /** {@inheritDoc} */
    @Override public Adder startAdding() throws GridException {
        return new AdderImpl();
    }

    /** {@inheritDoc} */
    @Override public GridHadoopTaskInput input() throws GridException {
        return null; // TODO
    }

    /**
     * @param meta Meta pointer.
     * @return Key pointer.
     */
    private long key(long meta) {
        return mem.readLong(meta);
    }

    /**
     * @param meta Meta pointer.
     * @param key Key pointer.
     */
    private void key(long meta, long key) {
        mem.writeLong(meta, key);
    }

    /**
     * @param meta Meta pointer.
     * @return Value pointer.
     */
    private long value(long meta) {
        return mem.readLongVolatile(meta + 8);
    }

    /**
     * @param meta Meta pointer.
     * @param valPtr Value pointer.
     */
    private void value(long meta, long valPtr) {
        mem.writeLongVolatile(meta + 8, valPtr);
    }

    /**
     * @param meta Meta pointer.
     * @param oldValPtr Old first value pointer.
     * @param newValPtr New first value pointer.
     * @return {@code true} If operation succeeded.
     */
    private boolean casValue(long meta, long oldValPtr, long newValPtr) {
        return mem.casLong(meta + 8, oldValPtr, newValPtr);
    }

    /**
     * @param meta Meta pointer.
     * @param level Level.
     * @return Next meta pointer.
     */
    private long nextMeta(long meta, int level) {
        return meta == 0 ? heads.get(level) : mem.readLongVolatile(meta + 16 + 8 * level);
    }

    /**
     * @param meta Meta pointer.
     * @param level Level.
     * @param oldNext Old next meta pointer.
     * @param newNext New next meta pointer.
     * @return {@code true} If operation succeeded.
     */
    private boolean casNextMeta(long meta, int level, long oldNext, long newNext) {
        return meta == 0 ? heads.compareAndSet(level, oldNext, newNext) :
            mem.casLong(meta + 16 + 8 * level, oldNext, newNext);
    }

    /**
     * @param meta Meta pointer.
     * @param nextMeta Next meta.
     */
    private void nextMeta(long meta, long nextMeta) {
        mem.writeLong(meta + 16, nextMeta);
    }

    /**
     * @param keyPtr Key pointer.
     * @return Key size.
     */
    private int keySize(long keyPtr) {
        return mem.readInt(keyPtr);
    }

    /**
     * @param keyPtr Key pointer.
     * @param keySize Key size.
     */
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
         * @param prevMeta Previous meta.
         * @param meta Next meta.
         */
        private void stackPush(long prevMeta, long meta) {
            stack.add(prevMeta);
            stack.add(meta);
        }

        /**
         * @return Upper meta from the stack.
         */
        private long upperMeta() {
            return stack.isEmpty() ? -1L : stack.last();
        }

        /**
         * @param key Key.
         * @param val Value.
         * @return Meta pointer.
         * @throws GridException If failed.
         */
        @SuppressWarnings("unchecked")
        private long add(Object key, @Nullable Object val) throws GridException {
            stack.clear();

            long valPtr = 0;

            if (val != null) { // Write value.
                valPtr = write(12, val, valSer);
                int valSize = writtenSize() - 12;

                valueSize(valPtr, valSize);
            }

            long keyPtr = 0;
            long newMeta = 0;
            int newMetaLevel = -1;

            long prevMeta = 0;
            int level = topLevel.get();
            long meta = level < 0 ? 0 : heads.get(level);

            for (int cmpRes;;) {
                if (level < 0) { // We did not find our key, trying to add new meta.
                    if (keyPtr == 0) { // Write key and create meta only once.
                        keyPtr = writeKey(key);

                        newMetaLevel = randomLevel(rnd);
                        newMeta = createMeta(keyPtr, valPtr, newMetaLevel);
                    }

                    nextMeta(newMeta, meta);

                    if (casNextMeta(prevMeta, 0, meta, newMeta)) { // The new key was added successfully.
                        laceUp(prevMeta, newMeta, newMetaLevel);

                        return newMeta;
                    }
                    else // Add failed, need to check out what was added by another thread.
                        meta = nextMeta(prevMeta, level = 0);
                }

                assert meta != 0;

//                if (meta != upperMeta()) { // TODO
                Object k = keyReader.readKey(meta);

                cmpRes = cmp.compare(key, k);
//                }

                if (cmpRes == 0) { // Key found.
                    if (newMeta != 0)  // Deallocate if we've allocated something.
                        localDeallocate(keyPtr);

                    if (valPtr == 0) // Only key needs to be added.
                        return meta;

                    for (;;) { // Add value for the key found.
                        long nextVal = value(meta);

                        nextValue(valPtr, nextVal);

                        if (casValue(meta, nextVal, valPtr))
                            return meta;
                    }
                }

                assert cmpRes != 0;

                if (cmpRes > 0) { // Go right.
                    prevMeta = meta;
                    meta = nextMeta(meta, level);

                    if (meta != 0) // If nothing to the right then go down.
                        continue;
                }

                while (--level >= 0) { // Go down.
                    stackPush(prevMeta, meta); // Remember the path.

                    meta = nextMeta(prevMeta, level);

                    if (meta != 0) // Else go deeper.
                        break;
                }
            }
        }

        /**
         * Adds appropriate index links between metas.
         *
         * @param prevMeta Previous meta.
         * @param newMeta Just added meta.
         * @param newMetaLevel New level.
         */
        private void laceUp(long prevMeta, long newMeta, int newMetaLevel) {
            if (stack.isEmpty()) //

            for (int i = 0; i < newMetaLevel; i+= 2) {
                if (stack.isEmpty()) {


                }

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
