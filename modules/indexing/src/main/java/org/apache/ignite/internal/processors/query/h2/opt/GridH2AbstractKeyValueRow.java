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

import java.lang.ref.WeakReference;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.jetbrains.annotations.Nullable;

/**
 * Table row implementation based on {@link GridQueryTypeDescriptor}.
 */
public abstract class GridH2AbstractKeyValueRow extends GridH2Row {
    /** */
    private static final int DEFAULT_COLUMNS_COUNT = 2;

    /** Key column. */
    public static final int KEY_COL = 0;

    /** Value column. */
    public static final int VAL_COL = 1;

    /** */
    protected final GridH2RowDescriptor desc;

    /** */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    protected long expirationTime;

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
    protected GridH2AbstractKeyValueRow(GridH2RowDescriptor desc, Object key, int keyType, @Nullable Object val,
        int valType, long expirationTime) throws IgniteCheckedException {
        super(null, null);

        setValue(KEY_COL, desc.wrap(key, keyType));

        if (val != null) // We remove by key only, so value can be null here.
            setValue(VAL_COL, desc.wrap(val, valType));

        this.desc = desc;
        this.expirationTime = expirationTime;
    }

    /**
     * Protected constructor for {@link GridH2KeyValueRowOffheap}
     *
     * @param desc Row descriptor.
     */
    protected GridH2AbstractKeyValueRow(GridH2RowDescriptor desc) {
        super(new Value[DEFAULT_COLUMNS_COUNT]);

        this.desc = desc;
    }

    /**
     * @return Expiration time of respective cache entry.
     */
    public long expirationTime() {
        return expirationTime;
    }

    /** {@inheritDoc} */
    @Override public int getColumnCount() {
        return DEFAULT_COLUMNS_COUNT + desc.fieldsCount();
    }

    /**
     * Should be called to remove reference on value.
     *
     * @throws IgniteCheckedException If failed.
     */
    public synchronized void onSwap() throws IgniteCheckedException {
        setValue(VAL_COL, null);
    }

    /**
     * Should be called when entry getting unswapped.
     *
     * @param val Value.
     * @param beforeRmv If this is unswap before remove.
     * @throws IgniteCheckedException If failed.
     */
    public synchronized void onUnswap(Object val, boolean beforeRmv) throws IgniteCheckedException {
        setValue(VAL_COL, desc.wrap(val, desc.valueType()));

        notifyAll();
    }

    /**
     * Atomically updates weak value.
     *
     * @param upd New value.
     * @return {@code null} If update succeeded, unexpected value otherwise.
     */
    protected synchronized Value updateWeakValue(Value upd) {
        Value res = peekValue(VAL_COL);

        if (res != null && !(res instanceof WeakValue))
            return res;

        setValue(VAL_COL, new WeakValue(upd));

        notifyAll();

        return null;
    }

    /**
     * @param waitTime Time to await for value unswap.
     * @return Synchronized value.
     */
    protected synchronized Value syncValue(long waitTime) {
        Value v = peekValue(VAL_COL);

        while (v == null && waitTime > 0) {
            long start = System.nanoTime(); // This call must be quite rare, so performance is not a concern.

            try {
                wait(waitTime); // Wait for value arrival to allow other threads to make a progress.
            }
            catch (InterruptedException e) {
                throw new IgniteInterruptedException(e);
            }

            long t = System.nanoTime() - start;

            if (t > 0)
                waitTime -= TimeUnit.NANOSECONDS.toMillis(t);

            v = peekValue(VAL_COL);
        }

        return v;
    }

    /**
     * @param col Column index.
     * @return Value if exists.
     */
    protected final Value peekValue(int col) {
        return getValueList()[col];
    }

    /** {@inheritDoc} */
    @Override public Value getValue(int col) {
        if (col < DEFAULT_COLUMNS_COUNT) {
            Value v = peekValue(col);

            if (col == VAL_COL) {
                long start = 0;
                int attempt = 0;

                while ((v = WeakValue.unwrap(v)) == null) {
                    v = getOffheapValue(VAL_COL);

                    if (v != null) {
                        setValue(VAL_COL, v);

                        if (peekValue(KEY_COL) == null)
                            cache();

                        return v;
                    }

                    Object k = getValue(KEY_COL).getObject();

                    try {
                        Object valObj = desc.readFromSwap(k);

                        if (valObj != null) {
                            Value upd = desc.wrap(valObj, desc.valueType());

                            v = updateWeakValue(upd);

                            return v == null ? upd : v;
                        }
                        else {
                            // If nothing found in swap then we should be already unswapped.
                            v = syncValue(attempt);
                        }
                    }
                    catch (IgniteCheckedException e) {
                        throw new IgniteException(e);
                    }

                    attempt++;

                    if (start == 0)
                        start = U.currentTimeMillis();
                    else if (U.currentTimeMillis() - start > 60_000) // Loop for at most 60 seconds.
                        throw new IgniteException("Failed to get value for key: " + k +
                            ". This can happen due to a long GC pause.");
                }
            }

            if (v == null) {
                assert col == KEY_COL : col;

                v = getOffheapValue(KEY_COL);

                assert v != null : v;

                setValue(KEY_COL, v);

                if (peekValue(VAL_COL) == null)
                    cache();
            }

            assert !(v instanceof WeakValue) : v;

            return v;
        }

        col -= DEFAULT_COLUMNS_COUNT;

        assert col >= 0;

        Value key = getValue(KEY_COL);
        Value val = getValue(VAL_COL);

        assert key != null;
        assert val != null;

        Object res = desc.columnValue(key.getObject(), val.getObject(), col);

        if (res == null)
            return ValueNull.INSTANCE;

        try {
            return desc.wrap(res, desc.fieldType(col));
        }
        catch (IgniteCheckedException e) {
            throw DbException.convert(e);
        }
    }

    /**
     * Caches this row for reuse.
     */
    protected abstract void cache();

    /**
     * @param col Column.
     * @return Value read from offheap memory or null if it is impossible.
     */
    protected abstract Value getOffheapValue(int col);

    /**
     * Adds offheap row ID.
     */
    protected void addOffheapRowId(SB sb) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        SB sb = new SB("Row@");

        sb.a(Integer.toHexString(System.identityHashCode(this)));

        addOffheapRowId(sb);

        Value v = peekValue(KEY_COL);
        sb.a("[ key: ").a(v == null ? "nil" : v.getString());

        v = WeakValue.unwrap(peekValue(VAL_COL));
        sb.a(", val: ").a(v == null ? "nil" : v.getString());

        sb.a(" ][ ");

        if (v != null) {
            for (int i = 2, cnt = getColumnCount(); i < cnt; i++) {
                v = getValue(i);

                if (i != 2)
                    sb.a(", ");

                sb.a(v == null ? "nil" : v.getString());
            }
        }

        sb.a(" ]");

        return sb.toString();
    }

    /** {@inheritDoc} */
    @Override public void setKeyAndVersion(SearchRow old) {
        assert false;
    }

    /** {@inheritDoc} */
    @Override public void setKey(long key) {
        assert false;
    }

    /** {@inheritDoc} */
    @Override public Row getCopy() {
        assert false;

        return null;
    }

    /** {@inheritDoc} */
    @Override public void setDeleted(boolean deleted) {
        assert false;
    }

    /** {@inheritDoc} */
    @Override public long getKey() {
        assert false;

        return 0;
    }

    /** {@inheritDoc} */
    @Override public void setSessionId(int sesId) {
        assert false;
    }

    /** {@inheritDoc} */
    @Override public void setVersion(int ver) {
        assert false;
    }

    /**
     * Weak reference to value that was swapped but accessed in indexing SPI.
     */
    private static class WeakValue extends Value {
        /**
         * Unwraps value.
         *
         * @param v Value.
         * @return Unwrapped value.
         */
        static Value unwrap(Value v) {
            return (v instanceof WeakValue) ? ((WeakValue)v).get() : v;
        }

        /** */
        private final WeakReference<Value> ref;

        /**
         * @param v Value.
         */
        private WeakValue(Value v) {
            ref = new WeakReference<>(v);
        }

        /**
         * @return Referenced value.
         */
        public Value get() {
            return ref.get();
        }

        /** {@inheritDoc} */
        @Override public String getSQL() {
            throw new IllegalStateException();
        }

        /** {@inheritDoc} */
        @Override public int getType() {
            throw new IllegalStateException();
        }

        /** {@inheritDoc} */
        @Override public long getPrecision() {
            throw new IllegalStateException();
        }

        /** {@inheritDoc} */
        @Override public int getDisplaySize() {
            throw new IllegalStateException();
        }

        /** {@inheritDoc} */
        @Override public String getString() {
            throw new IllegalStateException();
        }

        /** {@inheritDoc} */
        @Override public Object getObject() {
            throw new IllegalStateException();
        }

        /** {@inheritDoc} */
        @Override public void set(PreparedStatement preparedStatement, int i) throws SQLException {
            throw new IllegalStateException();
        }

        /** {@inheritDoc} */
        @Override protected int compareSecure(Value val, CompareMode compareMode) {
            throw new IllegalStateException();
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            throw new IllegalStateException();
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            throw new IllegalStateException();
        }
    }
}