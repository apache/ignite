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
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.hadoop.HadoopJobInfo;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskContext;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskInput;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.GridRandom;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Multimap for map reduce intermediate results.
 */
public class HadoopConcurrentHashMultimap extends HadoopHashMultimapBase {
    /** */
    private final AtomicReference<State> state = new AtomicReference<>(State.READING_WRITING);

    /** */
    private volatile AtomicLongArray oldTbl;

    /** */
    private volatile AtomicLongArray newTbl;

    /** */
    private final AtomicInteger keys = new AtomicInteger();

    /** */
    private final CopyOnWriteArrayList<AdderImpl> adders = new CopyOnWriteArrayList<>();

    /** */
    private final AtomicInteger inputs = new AtomicInteger();

    /**
     * @param jobInfo Job info.
     * @param mem Memory.
     * @param cap Initial capacity.
     */
    public HadoopConcurrentHashMultimap(HadoopJobInfo jobInfo, GridUnsafeMemory mem, int cap) {
        super(jobInfo, mem);

        assert U.isPow2(cap);

        newTbl = oldTbl = new AtomicLongArray(cap);
    }

    /**
     * @return Number of keys.
     */
    public long keys() {
        int res = keys.get();

        for (AdderImpl adder : adders)
            res += adder.locKeys.get();

        return res;
    }

    /**
     * @return Current table capacity.
     */
    @Override public int capacity() {
        return oldTbl.length();
    }

    /**
     * @return Adder object.
     * @param ctx Task context.
     */
    @Override public Adder startAdding(HadoopTaskContext ctx) throws IgniteCheckedException {
        if (inputs.get() != 0)
            throw new IllegalStateException("Active inputs.");

        if (state.get() == State.CLOSING)
            throw new IllegalStateException("Closed.");

        return new AdderImpl(ctx);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        assert inputs.get() == 0 : inputs.get();
        assert adders.isEmpty() : adders.size();

        state(State.READING_WRITING, State.CLOSING);

        if (keys() == 0)
            return;

        super.close();
    }

    /** {@inheritDoc} */
    @Override protected long meta(int idx) {
        return oldTbl.get(idx);
    }

    /**
     * Incrementally visits all the keys and values in the map.
     *
     * @param ignoreLastVisited Flag indicating that visiting must be started from the beginning.
     * @param v Visitor.
     * @return {@code false} If visiting was impossible due to rehashing.
     */
    @Override public boolean visit(boolean ignoreLastVisited, Visitor v) throws IgniteCheckedException {
        if (!state.compareAndSet(State.READING_WRITING, State.VISITING)) {
            assert state.get() != State.CLOSING;

            return false; // Can not visit while rehashing happens.
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

        return true;
    }

    /** {@inheritDoc} */
    @Override public HadoopTaskInput input(HadoopTaskContext taskCtx) throws IgniteCheckedException {
        inputs.incrementAndGet();

        if (!adders.isEmpty())
            throw new IllegalStateException("Active adders.");

        State s = state.get();

        if (s == State.CLOSING)
            throw new IllegalStateException("Closed.");

        assert s != State.REHASHING;

        return new Input(taskCtx) {
            @Override public void close() throws IgniteCheckedException {
                if (inputs.decrementAndGet() < 0)
                    throw new IllegalStateException();

                super.close();
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

        if (fromTbl != newTbl) // Check if someone else have done the job.
            return;

        if (!state.compareAndSet(State.READING_WRITING, State.REHASHING)) {
            assert state.get() != State.CLOSING; // Visiting is allowed, but we will not rehash.

            return;
        }

        if (fromTbl != newTbl) { // Double check.
            state(State.REHASHING, State.READING_WRITING); // Switch back.

            return;
        }

        // Calculate new table capacity.
        int newLen = fromTbl.length();

        do {
            newLen <<= 1;
        }
        while (newLen < keys0);

        if (keys0 >= 3 * (newLen >>> 2)) // Still more than 3/4.
            newLen <<= 1;

        // This is our target table for rehashing.
        AtomicLongArray toTbl = new AtomicLongArray(newLen);

        // Make the new table visible before rehashing.
        newTbl = toTbl;

        // Rehash.
        int newMask = newLen - 1;

        long failedMeta = 0;

        GridLongList collisions = new GridLongList(16);

        for (int i = 0; i < fromTbl.length(); i++) { // Scan source table.
            long meta = fromTbl.get(i);

            assert meta != -1;

            if (meta == 0) { // No entry.
                failedMeta = 0;

                if (!fromTbl.compareAndSet(i, 0, -1)) // Mark as moved.
                    i--; // Retry.

                continue;
            }

            do { // Collect all the collisions before the last one failed to nullify or 0.
                collisions.add(meta);

                meta = collision(meta);
            }
            while (meta != failedMeta);

            do { // Go from the last to the first to avoid 'in-flight' state for meta entries.
                meta = collisions.remove();

                int addr = keyHash(meta) & newMask;

                for (;;) { // Move meta entry to the new table.
                    long toCollision = toTbl.get(addr);

                    collision(meta, toCollision);

                    if (toTbl.compareAndSet(addr, toCollision, meta))
                        break;
                }
            }
            while (!collisions.isEmpty());

            // Here 'meta' will be a root pointer in old table.
            if (!fromTbl.compareAndSet(i, meta, -1)) { // Try to mark as moved.
                failedMeta = meta;

                i--; // Retry the same address in table because new keys were added.
            }
            else
                failedMeta = 0;
        }

        // Now old and new tables will be the same again.
        oldTbl = toTbl;

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
     * @param meta Meta pointer.
     * @return Value pointer.
     */
    @Override protected long value(long meta) {
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
    @Override protected long collision(long meta) {
        return mem.readLongVolatile(meta + 24);
    }

    /**
     * @param meta Meta pointer.
     * @param collision Collision pointer.
     */
    @Override protected void collision(long meta, long collision) {
        assert meta != collision : meta;

        mem.writeLongVolatile(meta + 24, collision);
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
     * Adder. Must not be shared between threads.
     */
    private class AdderImpl extends AdderBase {
        /** */
        private final Reader keyReader;

        /** */
        private final AtomicInteger locKeys = new AtomicInteger();

        /** */
        private final Random rnd = new GridRandom();

        /**
         * @param ctx Task context.
         * @throws IgniteCheckedException If failed.
         */
        private AdderImpl(HadoopTaskContext ctx) throws IgniteCheckedException {
            super(ctx);

            keyReader = new Reader(keySer);

            rehashIfNeeded(oldTbl);

            adders.add(this);
        }

        /**
         * @param in Data input.
         * @param reuse Reusable key.
         * @return Key.
         * @throws IgniteCheckedException If failed.
         */
        @Override public Key addKey(DataInput in, @Nullable Key reuse) throws IgniteCheckedException {
            KeyImpl k = reuse == null ? new KeyImpl() : (KeyImpl)reuse;

            k.tmpKey = keySer.read(in, k.tmpKey);

            k.meta = add(k.tmpKey, null);

            return k;
        }

        /** {@inheritDoc} */
        @Override public void write(Object key, Object val) throws IgniteCheckedException {
            A.notNull(val, "val");

            add(key, val);
        }

        /**
         * @param tbl Table.
         */
        private void incrementKeys(AtomicLongArray tbl) {
            locKeys.lazySet(locKeys.get() + 1);

            if (rnd.nextInt(tbl.length()) < 512)
                rehashIfNeeded(tbl);
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
            long meta = allocate(40);

            mem.writeInt(meta, keyHash);
            mem.writeInt(meta + 4, keySize);
            mem.writeLong(meta + 8, keyPtr);
            mem.writeLong(meta + 16, valPtr);
            mem.writeLong(meta + 24, collisionPtr);
            mem.writeLong(meta + 32, lastVisitedVal);

            return meta;
        }

        /**
         * @param key Key.
         * @param val Value.
         * @return Updated or created meta page pointer.
         * @throws IgniteCheckedException If failed.
         */
        private long add(Object key, @Nullable Object val) throws IgniteCheckedException {
            AtomicLongArray tbl = oldTbl;

            int keyHash = U.hash(key.hashCode());

            long newMetaPtr = 0;

            long valPtr = 0;

            if (val != null) {
                valPtr = write(12, val, valSer);
                int valSize = writtenSize() - 12;

                valueSize(valPtr, valSize);
            }

            for (AtomicLongArray old = null;;) {
                int addr = keyHash & (tbl.length() - 1);

                long metaPtrRoot = tbl.get(addr); // Read root meta pointer at this address.

                if (metaPtrRoot == -1) { // The cell was already moved by rehashing.
                    AtomicLongArray n = newTbl; // Need to read newTbl first here.
                    AtomicLongArray o = oldTbl;

                    tbl = tbl == o ? n : o; // Trying to get the oldest table but newer than ours.

                    old = null;

                    continue;
                }

                if (metaPtrRoot != 0) { // Not empty slot.
                    long metaPtr = metaPtrRoot;

                    do { // Scan all the collisions.
                        if (keyHash(metaPtr) == keyHash && key.equals(keyReader.readKey(metaPtr))) { // Found key.
                            if (newMetaPtr != 0)  // Deallocate new meta if one was allocated.
                                localDeallocate(key(newMetaPtr)); // Key was allocated first, so rewind to it's pointer.

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

                    // Here we did not find our key, need to check if it was moved by rehashing to the new table.
                    if (old == null) { // If the old table already set, then we will just try to update it.
                        AtomicLongArray n = newTbl;

                        if (n != tbl) { // Rehashing happens, try to find the key in new table but preserve the old one.
                            old = tbl;
                            tbl = n;

                            continue;
                        }
                    }
                }

                if (old != null) { // We just checked new table but did not find our key as well as in the old one.
                    tbl = old; // Try to add new key to the old table.

                    addr = keyHash & (tbl.length() - 1);

                    old = null;
                }

                if (newMetaPtr == 0) { // Allocate new meta page.
                    long keyPtr = write(0, key, keySer);
                    int keySize = writtenSize();

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

        /** {@inheritDoc} */
        @Override public void close() throws IgniteCheckedException {
            if (!adders.remove(this))
                throw new IllegalStateException();

            keys.addAndGet(locKeys.get()); // Here we have race and #keys() method can return wrong result but it is ok.

            super.close();
        }

        /**
         * Key.
         */
        private class KeyImpl implements Key {
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

    /**
     * Current map state.
     */
    private enum State {
        /** */
        REHASHING,

        /** */
        VISITING,

        /** */
        READING_WRITING,

        /** */
        CLOSING
    }
}