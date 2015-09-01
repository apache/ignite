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

package org.apache.ignite.internal.processors.query.h2.opt;

import java.util.concurrent.locks.Lock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.GridStripedLock;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.h2.store.Data;
import org.h2.value.Value;
import org.jetbrains.annotations.Nullable;

/**
 * Offheap row.
 */
public class GridH2KeyValueRowOffheap extends GridH2AbstractKeyValueRow {
    /** */
    private static final GridStripedLock lock;

    /**
     * Init locks.
     */
    static {
        int cpus = Runtime.getRuntime().availableProcessors();

        lock = new GridStripedLock(cpus * cpus * 8);
    }

    /** */
    private static final int OFFSET_KEY_SIZE = 4; // 4 after ref cnt int

    /** */
    private static final int OFFSET_VALUE_REF = OFFSET_KEY_SIZE + 4; // 8

    /** */
    private static final int OFFSET_EXPIRATION = OFFSET_VALUE_REF + 8; // 16

    /** */
    private static final int OFFSET_KEY = OFFSET_EXPIRATION + 8; // 24

    /** */
    private static final int OFFSET_VALUE = 4; // 4 on separate page after val size int

    /** */
    private static final Data SIZE_CALCULATOR = Data.create(null, null);

    /** */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private long ptr;

    /**
     * @param desc Row descriptor.
     * @param ptr Pointer.
     */
    public GridH2KeyValueRowOffheap(GridH2RowDescriptor desc, long ptr) {
        super(desc);

        assert ptr > 0 : ptr;

        this.ptr = ptr;
    }

    /**
     * Constructor.
     *
     * @param desc Row descriptor.
     * @param key Key.
     * @param keyType Key type.
     * @param val Value.
     * @param valType Value type.
     * @param expirationTime Expiration time.
     * @throws IgniteCheckedException If failed.
     */
    public GridH2KeyValueRowOffheap(GridH2RowDescriptor desc, Object key, int keyType, @Nullable Object val, int valType,
        long expirationTime) throws IgniteCheckedException {
        super(desc, key, keyType, val, valType, expirationTime);
    }

    /** {@inheritDoc} */
    @Override public long expirationTime() {
        if (expirationTime == 0) {
            long p = ptr;

            assert p > 0 : p;

            // We don't need any synchronization or volatility here because we publish via
            // volatile write to tree node.
            expirationTime = desc.memory().readLong(p + OFFSET_EXPIRATION);
        }

        return expirationTime;
    }

    /** {@inheritDoc} */
    @Override protected void cache() {
        desc.cache(this);
    }

    /**
     * @param ptr Pointer to get lock for.
     * @return Locked lock, must be released in {@code finally} block.
     */
    @SuppressWarnings("LockAcquiredButNotSafelyReleased")
    private static Lock lock(long ptr) {
        assert ptr > 0 : ptr;
        assert (ptr & 7) == 0 : ptr; // Unsafe allocated pointers aligned.

        Lock l = lock.getLock(ptr >>> 3);

        l.lock();

        return l;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("LockAcquiredButNotSafelyReleased")
    @Override protected Value getOffheapValue(int col) {
        GridUnsafeMemory mem = desc.memory();

        long p = ptr;

        assert p > 0 : p;

        byte[] bytes = null;

        if (col == KEY_COL) {
            int size = mem.readInt(p + OFFSET_KEY_SIZE);

            assert size > 0 : size;

            bytes = mem.readBytes(p + OFFSET_KEY, size);
        }
        else if (col == VAL_COL) {
            Lock l = lock(p);

            desc.guard().begin();

            try {
                long valPtr = mem.readLongVolatile(p + OFFSET_VALUE_REF);

                if (valPtr == 0) // Value was evicted.
                    return null;

                int size = mem.readInt(valPtr);

                assert size > 0 : size;

                bytes = mem.readBytes(valPtr + OFFSET_VALUE, size);
            }
            finally {
                desc.guard().end();

                l.unlock();
            }
        }
        else
            assert false : col;

        Data data = Data.create(null, bytes);

        return data.readValue();
    }

    /** {@inheritDoc} */
    @Override public long pointer() {
        long p = ptr;

        assert p > 0: p;

        return p;
    }

    /** {@inheritDoc} */
    @Override public synchronized void onSwap() throws IgniteCheckedException {
        Lock l = lock(ptr);

        try {
            final long p = ptr + OFFSET_VALUE_REF;

            final GridUnsafeMemory mem = desc.memory();

            final long valPtr = mem.readLongVolatile(p);

            if (valPtr <= 0)
                throw new IllegalStateException("Already swapped: " + ptr);

            if (!mem.casLong(p, valPtr, 0))
                throw new IllegalStateException("Concurrent unswap: " + ptr);

            desc.guard().finalizeLater(new Runnable() {
                @Override public void run() {
                    mem.release(valPtr, mem.readInt(valPtr) + OFFSET_VALUE);
                }
            });
        }
        finally {
            l.unlock();
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("NonSynchronizedMethodOverridesSynchronizedMethod")
    @Override protected synchronized Value updateWeakValue(Value upd) {
        setValue(VAL_COL, upd);

        notifyAll();

        return null;
    }

    /** {@inheritDoc} */
    @Override public synchronized void onUnswap(Object val, boolean beforeRmv) throws IgniteCheckedException {
        assert val != null;

        final long p = ptr;

        Lock l = lock(p);

        try {
            GridUnsafeMemory mem = desc.memory();

            if (mem.readLongVolatile(p + OFFSET_VALUE_REF) != 0)
                return; // The offheap value is in its place, nothing to do here.

            Value v = peekValue(VAL_COL);

            if (v == null) {
                setValue(VAL_COL, desc.wrap(val, desc.valueType()));

                v = peekValue(VAL_COL);
            }

            byte[] bytes = new byte[SIZE_CALCULATOR.getValueLen(v)];

            Data data = Data.create(null, bytes);

            data.writeValue(v);

            long valPtr = mem.allocate(bytes.length + OFFSET_VALUE);

            mem.writeInt(valPtr, bytes.length);
            mem.writeBytes(valPtr + OFFSET_VALUE, bytes);

            mem.writeLongVolatile(p + OFFSET_VALUE_REF, valPtr);
        }
        finally {
            l.unlock();
        }

        notifyAll();
    }

    /** {@inheritDoc} */
    @Override protected Value syncValue(long waitTime) {
        Value v = super.syncValue(waitTime);

        if (v != null)
            return v;

        return getOffheapValue(VAL_COL);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"NonPrivateFieldAccessedInSynchronizedContext"})
    @Override public void incrementRefCount() {
        long p = ptr;

        GridUnsafeMemory mem = desc.memory();

        if (p == 0) { // Serialize data to offheap memory.
            Value key = peekValue(KEY_COL);
            Value val = peekValue(VAL_COL);

            assert key != null;
            assert val != null;

            Data data = Data.create(null, new byte[SIZE_CALCULATOR.getValueLen(key)]);

            data.writeValue(key);

            int keySize = data.length();

            p = mem.allocate(keySize + OFFSET_KEY);

            // We don't need any synchronization or volatility here because we publish via
            // volatile write to tree node.
            mem.writeInt(p, 1);
            mem.writeLong(p + OFFSET_EXPIRATION, expirationTime);
            mem.writeInt(p + OFFSET_KEY_SIZE, keySize);
            mem.writeBytes(p + OFFSET_KEY, data.getBytes(), 0, keySize);

            data = Data.create(null, new byte[SIZE_CALCULATOR.getValueLen(val)]);

            data.writeValue(val);

            int valSize = data.length();

            long valPtr = mem.allocate(valSize + OFFSET_VALUE);

            mem.writeInt(valPtr, valSize);
            mem.writeBytes(valPtr + OFFSET_VALUE, data.getBytes(), 0, valSize);

            mem.writeLongVolatile(p + OFFSET_VALUE_REF, valPtr);

            ptr = p;

            desc.cache(this);
        }
        else {
            for (;;) {
                int cnt = mem.readIntVolatile(p);

                assert cnt > 0 : cnt;

                if (mem.casInt(p, cnt, cnt + 1))
                    break;
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void decrementRefCount() {
        long p = ptr;

        assert p > 0 : p;

        GridUnsafeMemory mem = desc.memory();

        for (;;) {
            int cnt = mem.readIntVolatile(p);

            assert cnt > 0 : cnt;

            if (cnt == 1)
                break;

            if (mem.casInt(p, cnt, cnt - 1))
                return;
        }

        desc.uncache(p);

        // Deallocate off-heap memory.
        long valPtr = mem.readLongVolatile(p + OFFSET_VALUE_REF);

        assert valPtr >= 0 : valPtr;

        if (valPtr != 0)
            mem.release(valPtr, mem.readInt(valPtr) + OFFSET_VALUE);

        mem.release(p, mem.readInt(p + OFFSET_KEY_SIZE) + OFFSET_KEY);
    }

    /** {@inheritDoc} */
    @Override protected void addOffheapRowId(SB sb) {
        sb.a('-').a(ptr);
    }
}