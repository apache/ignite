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
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.io.*;
import org.gridgain.grid.util.offheap.unsafe.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.util.offheap.unsafe.GridUnsafeMemory.*;

/**
 * Multimap for map reduce intermediate results.
 */
public class GridHadoopMultimap implements AutoCloseable {
    /** */
    private static AtomicIntegerFieldUpdater<Adder> keysCntUpdater =
        AtomicIntegerFieldUpdater.newUpdater(Adder.class, "keysCnt");

    /** */
    private final AtomicReference<State> state = new AtomicReference<>(State.READING_WRITING);

    /** */
    private volatile AtomicLongArray oldTbl;

    /** */
    private volatile AtomicLongArray newTbl;

    /** */
    private final AtomicInteger keys = new AtomicInteger();

    /** */
    private final CopyOnWriteArrayList<Adder> adders = new CopyOnWriteArrayList<>();

    /** */
    private final AtomicInteger inputs = new AtomicInteger();

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

        oldTbl = new AtomicLongArray(tblCap);
    }

    /**
     * @return Number of keys.
     */
    public long keys() {
        int res = keys.get();

        for (Adder adder : adders)
            res += adder.keysCnt;

        return res;
    }

    /**
     * @return Current table capacity.
     */
    public int capacity() {
        return oldTbl.length();
    }

    /**
     * @return Adder object.
     */
    public Adder startAdding() throws GridException {
        if (inputs.get() != 0)
            throw new IllegalStateException("Active inputs.");

        State s = state.get();

        if (s == State.CLOSING)
            throw new IllegalStateException();

        return new Adder();
    }

    /** {@inheritDoc} */
    @Override public void close() {
        assert state.get() == State.READING_WRITING;

        state(State.READING_WRITING, State.CLOSING);

        AtomicLongArray tbl0 = oldTbl;

        for (int i = 0; i < tbl0.length(); i++)  {
            long meta = tbl0.get(i);

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

                mem.release(meta0, 40);
            }
        }
    }

    /**
     * Incrementally visits all the keys and values in the map.
     *
     * @param ignoreLastVisited Flag indicating that visiting must be started from the beginning.
     * @param v Visitor.
     */
    public void visit(boolean ignoreLastVisited, Visitor v) throws GridException {
        if (!state.compareAndSet(State.READING_WRITING, State.VISITING)) {
            assert state.get() != State.CLOSING;

            return; // Can not visit while rehashing happens.
        }

        AtomicLongArray tbl0 = oldTbl;

        for (int i = 0; i < tbl0.length(); i++) {
            long meta = tbl0.get(i);

            while (meta != 0) {
                long valPtr = value(meta);

                long lastVisited = ignoreLastVisited ? 0 : lastVisitedValue(meta);

                if (valPtr != lastVisited) {
                    v.onKey(key(meta), keySize(meta));

                    lastVisitedValue(meta, valPtr); // Set it to the first value in chain.

                    do {
                        v.onValue(valPtr + 12, valueSize(valPtr));

                        valPtr = nextValue(valPtr);
                    }
                    while (valPtr != lastVisited);
                }

                meta = collision(meta);
            }
        }

        state(State.VISITING, State.READING_WRITING);
    }

    /**
     * @return New input for task.
     */
    public GridHadoopTaskInput input() throws GridException {
        inputs.incrementAndGet();

        if (!adders.isEmpty())
            throw new IllegalStateException("Active adders.");

        State s = state.get();

        if (s == State.CLOSING)
            throw new IllegalStateException("Closed.");

        assert s != State.REHASHING;

        final Reader keyReader = new Reader(job.keySerialization());
        final Reader valReader = new Reader(job.valueSerialization());

        final AtomicLongArray tbl = oldTbl;

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
                if (inputs.decrementAndGet() < 0)
                    throw new IllegalStateException();
            }
        };
    }

    /**
     * @param fromTbl Table.
     */
    private void rehashIfNeeded(AtomicLongArray fromTbl) {
        if (fromTbl.length() == Integer.MAX_VALUE)
            return;

        long keys0 = keys();

        if (keys0 < 3 * (fromTbl.length() >>> 2)) // New size has to be >= than 3/4 of capacity to rehash.
            return;

        if (fromTbl != oldTbl) // Check if someone else have done the job.
            return;

        if (!state.compareAndSet(State.READING_WRITING, State.REHASHING)) {
            assert state.get() != State.CLOSING; // Visiting is allowed, but we will not rehash.

            return;
        }

        if (fromTbl != oldTbl) { // Double check.
            state(State.REHASHING, State.READING_WRITING);

            return;
        }

        assert newTbl == null;

        // Calculate new table capacity.
        int newLen = fromTbl.length();

        do {
            newLen <<= 1;
        }
        while (newLen < keys0);

        if (keys0 >= 3 * (newLen >>> 2))
            newLen <<= 1;

        AtomicLongArray toTbl = new AtomicLongArray(newLen);

        newTbl = toTbl;

        // Rehash.

        int newMask = newLen - 1;

        GridLongList collisions = new GridLongList(16);

        for (int i = 0; i < fromTbl.length(); i++) { // Scan table.
            long meta = fromTbl.get(i);

            if (meta == -1)
                continue;

            if (meta == 0) { // No entry.
                if (!fromTbl.weakCompareAndSet(i, 0, -1)) // Mark as moved.
                    i--; // Retry.

                continue;
            }

            do { // Collect all the collisions.
                collisions.add(meta);

                meta = collision(meta);
            }
            while (meta != 0);

            int collisionsCnt = collisions.size();

            do { // Go from the last to the first.
                meta = collisions.get(--collisionsCnt);

                int addr = keyHash(meta) & newMask;

                for (;;) { // Move meta entry to the new table.
                    long toCollision = toTbl.get(addr);

                    collision(meta, toCollision);

                    if (toTbl.compareAndSet(addr, toCollision, meta))
                        break;
                }
            }
            while (collisionsCnt > 0);

            collisions.truncate(0, true);

            // Here 'meta' will be a root pointer in old table.
            if (!fromTbl.weakCompareAndSet(i, meta, -1)) // Try to mark as moved.
                i--; // Retry the same address in table because new keys were added.
        }

        oldTbl = toTbl;
        newTbl = null;

        state(State.REHASHING, State.READING_WRITING);
    }

    /**
     * Switch state.
     *
     * @param oldState Expected state.
     * @param newState New state.
     */
    private void state(State oldState, State newState) {
        if (!state.compareAndSet(oldState, newState))
            throw new IllegalStateException();
    }

    /**
     * @param keyHash Key hash.
     * @param keySize Key size.
     * @param keyPtr Key pointer.
     * @param valPtr Value page pointer.
     * @param collisionPtr Pointer to meta with hash collision.
     * @param lastVisitedVal Last visited value pointer.
     * @return Created meta page pointer.
     */
    private long createMeta(int keyHash, int keySize, long keyPtr, long valPtr, long collisionPtr, long lastVisitedVal) {
        long meta = mem.allocate(40);

        mem.writeInt(meta, keyHash);
        mem.writeInt(meta + 4, keySize);
        mem.writeLong(meta + 8, keyPtr);
        mem.writeLong(meta + 16, valPtr);
        mem.writeLong(meta + 24, collisionPtr);
        mem.writeLong(meta + 32, lastVisitedVal);

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
        return mem.readLongVolatile(meta + 16);
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
     * @param collision Collision pointer.
     */
    private void collision(long meta, long collision) {
        mem.writeLong(meta + 24, collision);
    }

    /**
     * @param meta Meta pointer.
     * @return Last visited value pointer.
     */
    private long lastVisitedValue(long meta) {
        return mem.readLong(meta + 32);
    }

    /**
     * @param meta Meta pointer.
     * @param valPtr Last visited value pointer.
     */
    private void lastVisitedValue(long meta, long valPtr) {
        mem.writeLong(meta + 32, valPtr);
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
        private final GridHadoopDataInStream in = new GridHadoopDataInStream(mem);

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

            try {
                return read(key(meta), keySize(meta));
            }
            catch (GridException e) {
                throw new GridRuntimeException(e);
            }
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
        private Object read(long ptr, long size) throws GridException {
            in.buffer().buffer(ptr, size);

            tmp = ser.read(in, tmp);

            return tmp;
        }
    }

    /**
     * Key.
     */
    public class Key {
        /** */
        private long meta;

        /** */
        private Object tmpKey;

        /**
         * @param val Value.
         */
        public void add(Value val) {
            int size = val.size();

            long valPtr = mem.allocate(size + 12);

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

    /**
     * Value.
     */
    public interface Value {
        /**
         * @return Size in bytes.
         */
        public int size();

        /**
         * @param ptr Pointer.
         */
        public void copyTo(long ptr);
    }

    /**
     * Adder. Must not be shared between threads.
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

        /** */
        volatile int keysCnt;

        /** */
        private final Random rnd = ThreadLocalRandom.current();

        /**
         * @throws GridException If failed.
         */
        public Adder() throws GridException {
            valSer = job.valueSerialization();
            keySer = job.keySerialization();

            keyReader = new Reader(keySer);

            rehashIfNeeded(oldTbl);

            adders.add(this);
        }

        /**
         * @param in Data input.
         * @param reuse Reusable key.
         * @return Key.
         * @throws GridException If failed.
         */
        public Key addKey(DataInput in, @Nullable Key reuse) throws GridException {
            if (reuse == null)
                reuse = new Key();

            reuse.tmpKey = keySer.read(in, reuse.tmpKey);

            reuse.meta = doAdd(reuse.tmpKey, null);

            return reuse;
        }

        /**
         * Adds value for the given key.
         *
         * @param key Key.
         * @param val Value.
         */
        public void add(Object key, Object val) throws GridException {
            A.notNull(val, "val");

            doAdd(key, val);
        }

        /**
         * @param tbl Table.
         */
        private void incrementKeys(AtomicLongArray tbl) {
            keysCntUpdater.lazySet(this, keysCnt + 1);

            if (rnd.nextInt(tbl.length()) < 512)
                rehashIfNeeded(tbl);
        }

        /**
         * @param key Key.
         * @param val Value.
         * @return Meta page pointer.
         * @throws GridException If failed.
         */
        private long doAdd(Object key, @Nullable Object val) throws GridException {
            AtomicLongArray tbl = oldTbl;

            int keyHash = U.hash(key.hashCode());

            long newMetaPtr = 0;

            long valPtr = 0;

            if (val != null) {
                write(val, valSer);

                int valSize = out.offset();

                valPtr = copy(12);

                valueSize(valPtr, valSize);
            }

            for (;;) {
                int addr = keyHash & (tbl.length() - 1);

                long metaPtrRoot = tbl.get(addr); // Read root meta pointer at this address.

                if (metaPtrRoot == -1) { // Rehashing.
                    tbl = newTbl;

                    if (tbl == null)
                        tbl = oldTbl;

                    continue;
                }

                if (metaPtrRoot != 0) { // Not empty slot.
                    long metaPtr = metaPtrRoot;

                    do { // Scan all the collisions.
                        if (keyHash(metaPtr) == keyHash && key.equals(keyReader.readKey(metaPtr))) { // Found key.
                            if (newMetaPtr != 0) { // Deallocate new meta if one was allocated.
                                mem.release(key(newMetaPtr), keySize(newMetaPtr));
                                mem.release(newMetaPtr, 40);
                            }

                            if (valPtr != 0) { // Add value if it exists.
                                long nextValPtr;

                                // Values are linked to each other to a stack like structure.
                                // Replace the last value in meta with ours and link it as next.
                                do {
                                    nextValPtr = value(metaPtr);

                                    nextValue(valPtr, nextValPtr);
                                }
                                while (!casValue(metaPtr, nextValPtr, valPtr));
                            }

                            return metaPtr;
                        }

                        metaPtr = collision(metaPtr);
                    }
                    while (metaPtr != 0);
                }

                if (newMetaPtr == 0) { // Allocate new meta page.
                    write(key, keySer);

                    int keySize = out.offset();

                    long keyPtr = copy(0);

                    if (valPtr != 0)
                        nextValue(valPtr, 0);

                    newMetaPtr = createMeta(keyHash, keySize, keyPtr, valPtr, metaPtrRoot, 0);
                }
                else // Update new meta with root pointer collision.
                    collision(newMetaPtr, metaPtrRoot);

                if (tbl.compareAndSet(addr, metaPtrRoot, newMetaPtr)) { // Try to replace root pointer with new one.
                    incrementKeys(tbl);

                    return newMetaPtr;
                }
            }
        }

        /**
         * @param o Object.
         */
        private void write(Object o, GridHadoopSerialization ser) throws GridException {
            out.reset();

            ser.write(out, o);
        }

        /**
         * @param off Offset.
         * @return Allocated pointer.
         */
        private long copy(int off) {
            int size = out.offset();

            long ptr = mem.allocate(off + size);

            UNSAFE.copyMemory(out.internalArray(), BYTE_ARR_OFF, null, ptr + off, size);

            return ptr;
        }

        /** {@inheritDoc} */
        @Override public void close() throws GridException {
            if (!adders.remove(this))
                throw new IllegalStateException();

            keys.addAndGet(keysCnt); // Here we have race and #keys() method can return wrong result but it is ok.

            keySer.close();
            valSer.close();
        }
    }

    /**
     * Key and values visitor.
     */
    public static interface Visitor {
        /**
         * @param keyPtr Key pointer.
         * @param keySize Key size.
         */
        public void onKey(long keyPtr, int keySize) throws GridException;

        /**
         * @param valPtr Value pointer.
         * @param valSize Value size.
         */
        public void onValue(long valPtr, int valSize) throws GridException;
    }

    /**
     * Current map state.
     */
    private static enum State {
        REHASHING, VISITING, READING_WRITING, CLOSING
    }
}
