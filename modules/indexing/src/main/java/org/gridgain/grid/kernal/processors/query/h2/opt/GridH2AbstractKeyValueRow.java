/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.query.h2.opt;

import org.apache.ignite.spi.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.query.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.h2.message.*;
import org.h2.result.*;
import org.h2.value.*;
import org.jetbrains.annotations.*;

import java.lang.ref.*;
import java.math.*;
import java.sql.Date;
import java.sql.*;
import java.util.*;

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
     * @throws IgniteSpiException If failed.
     */
    protected GridH2AbstractKeyValueRow(GridH2RowDescriptor desc, Object key, int keyType, @Nullable Object val,
        int valType, long expirationTime) throws IgniteSpiException {
        super(wrap(key, keyType),
            val == null ? null : wrap(val, valType)); // We remove by key only, so value can be null here.

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
     * Wraps object to respective {@link Value}.
     *
     * @param obj Object.
     * @param type Value type.
     * @return Value.
     * @throws IgniteSpiException If failed.
     */
    private static Value wrap(Object obj, int type) throws IgniteSpiException {
        switch (type) {
            case Value.BOOLEAN:
                return ValueBoolean.get((Boolean)obj);
            case Value.BYTE:
                return ValueByte.get((Byte)obj);
            case Value.SHORT:
                return ValueShort.get((Short)obj);
            case Value.INT:
                return ValueInt.get((Integer)obj);
            case Value.FLOAT:
                return ValueFloat.get((Float)obj);
            case Value.LONG:
                return ValueLong.get((Long)obj);
            case Value.DOUBLE:
                return ValueDouble.get((Double)obj);
            case Value.UUID:
                UUID uuid = (UUID)obj;
                return ValueUuid.get(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
            case Value.DATE:
                return ValueDate.get((Date)obj);
            case Value.TIME:
                return ValueTime.get((Time)obj);
            case Value.TIMESTAMP:
                if (obj instanceof java.util.Date && !(obj instanceof Timestamp))
                    obj = new Timestamp(((java.util.Date) obj).getTime());

                return GridH2Utils.toValueTimestamp((Timestamp)obj);
            case Value.DECIMAL:
                return ValueDecimal.get((BigDecimal)obj);
            case Value.STRING:
                return ValueString.get(obj.toString());
            case Value.BYTES:
                return ValueBytes.get((byte[])obj);
            case Value.JAVA_OBJECT:
                return ValueJavaObject.getNoCopy(obj, null, null);
            case Value.ARRAY:
                Object[] arr = (Object[])obj;

                Value[] valArr = new Value[arr.length];

                for (int i = 0; i < arr.length; i++) {
                    Object o = arr[i];

                    valArr[i] = o == null ? ValueNull.INSTANCE : wrap(o, DataType.getTypeFromClass(o.getClass()));
                }

                return ValueArray.get(valArr);

            case Value.GEOMETRY:
                return ValueGeometry.getFromGeometry(obj);
        }

        throw new IgniteSpiException("Failed to wrap value[type=" + type + ", value=" + obj + "]");
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
     * @throws IgniteSpiException If failed.
     */
    public synchronized void onSwap() throws GridException {
        setValue(VAL_COL, null);
    }

    /**
     * Should be called when entry getting unswapped.
     *
     * @param val Value.
     * @throws GridException If failed.
     */
    public synchronized void onUnswap(Object val) throws GridException {
        setValue(VAL_COL, wrap(val, desc.valueType()));
    }

    /**
     * Atomically updates weak value.
     *
     * @param exp Expected value.
     * @param upd New value.
     * @return Expected value if update succeeded, unexpected value otherwise.
     */
    protected synchronized Value updateWeakValue(Value exp, Value upd) {
        Value res = super.getValue(VAL_COL);

        if (res != exp && !(res instanceof WeakValue))
            return res;

        setValue(VAL_COL, new WeakValue(upd));

        return exp;
    }

    /**
     * @return Synchronized value.
     */
    protected synchronized Value syncValue() {
        return super.getValue(VAL_COL);
    }

    /** {@inheritDoc} */
    @Override public Value getValue(int col) {
        if (col < DEFAULT_COLUMNS_COUNT) {
            Value v = super.getValue(col);

            if (col == VAL_COL) {
                while ((v = WeakValue.unwrap(v)) == null) {
                    v = getOffheapValue(VAL_COL);

                    if (v != null) {
                        setValue(VAL_COL, v);

                        if (super.getValue(KEY_COL) == null)
                            cache();

                        return v;
                    }

                    try {
                        Object valObj = desc.readFromSwap(getValue(KEY_COL).getObject());

                        if (valObj != null) {
                            Value upd = wrap(valObj, desc.valueType());

                            Value res = updateWeakValue(v, upd);

                            if (res == v) {
                                if (super.getValue(KEY_COL) == null)
                                    cache();

                                return upd;
                            }

                            v = res;
                        }
                        else {
                            // If nothing found in swap then we should be already unswapped.
                            v = syncValue();
                        }
                    }
                    catch (GridException e) {
                        throw new GridRuntimeException(e);
                    }
                }
            }

            if (v == null) {
                assert col == KEY_COL : col;

                v = getOffheapValue(KEY_COL);

                assert v != null : v;

                setValue(KEY_COL, v);

                if (super.getValue(VAL_COL) == null)
                    cache();
            }

            assert !(v instanceof WeakValue) : v;

            return v;
        }

        col -= DEFAULT_COLUMNS_COUNT;

        assert col >= 0;

        Value v = getValue(desc.isKeyColumn(col) ? KEY_COL : VAL_COL);

        if (v == null)
            return null;

        Object obj = v.getObject();

        Object res = desc.columnValue(obj, col);

        if (res == null)
            return ValueNull.INSTANCE;

        try {
            return wrap(res, desc.fieldType(col));
        }
        catch (IgniteSpiException e) {
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

    /** {@inheritDoc} */
    @Override public String toString() {
        SB sb = new SB("Row@");

        sb.a(Integer.toHexString(System.identityHashCode(this)));

        Value v = super.getValue(KEY_COL);
        sb.a("[ key: ").a(v == null ? "nil" : v.getString());

        v = WeakValue.unwrap(super.getValue(VAL_COL));
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
