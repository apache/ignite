package org.apache.ignite.internal.processors.query.h2.opt;

import org.apache.ignite.*;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.*;
import org.h2.message.*;
import org.h2.util.*;
import org.h2.value.*;

import java.sql.*;

/**
 * H2 Value over {@link CacheObject}. Replacement for {@link ValueJavaObject}.
 */
public class GridH2ValueCacheObject extends Value {
    /** */
    private CacheObject obj;

    /** */
    private CacheObjectContext coctx;

    /**
     * @param coctx Cache object context.
     * @param obj Object.
     */
    public GridH2ValueCacheObject(CacheObjectContext coctx, CacheObject obj) {
        assert obj != null;

        this.obj = obj;
        this.coctx = coctx; // Allowed to be null in tests.
    }

    /**
     * @return Cache object.
     */
    public CacheObject getCacheObject() {
        return obj;
    }

    /** {@inheritDoc} */
    @Override public String getSQL() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int getType() {
        return Value.JAVA_OBJECT;
    }

    /** {@inheritDoc} */
    @Override public long getPrecision() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getDisplaySize() {
        return 64;
    }

    /** {@inheritDoc} */
    @Override public String getString() {
        return getObject().toString();
    }

    /** {@inheritDoc} */
    @Override public byte[] getBytes() {
        return Utils.cloneByteArray(getBytesNoCopy());
    }

    /** {@inheritDoc} */
    @Override public byte[] getBytesNoCopy() {
        if (obj.type() == CacheObject.TYPE_REGULAR) {
            // Result must be the same as `marshaller.marshall(obj.value(coctx, false));`
            try {
                return obj.valueBytes(coctx);
            }
            catch (IgniteCheckedException e) {
                throw DbException.convert(e);
            }
        }

        // For portables and byte array cache object types.
        return Utils.serialize(obj.value(coctx, false), null);
    }

    /** {@inheritDoc} */
    @Override public Object getObject() {
        return obj.value(coctx, false);
    }

    /** {@inheritDoc} */
    @Override public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        prep.setObject(parameterIndex, getObject(), Types.JAVA_OBJECT);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected int compareSecure(Value v, CompareMode mode) {
        Object o1 = getObject();
        Object o2 = v.getObject();

        boolean o1Comparable = o1 instanceof Comparable;
        boolean o2Comparable = o2 instanceof Comparable;

        if (o1Comparable && o2Comparable &&
            Utils.haveCommonComparableSuperclass(o1.getClass(), o2.getClass())) {
            Comparable<Object> c1 = (Comparable<Object>)o1;

            return c1.compareTo(o2);
        }

        // Group by types.
        if (o1.getClass() != o2.getClass()) {
            if (o1Comparable != o2Comparable)
                return o1Comparable ? -1 : 1;

            return o1.getClass().getName().compareTo(o2.getClass().getName());
        }

        // Compare hash codes.
        int h1 = hashCode();
        int h2 = v.hashCode();

        if (h1 == h2) {
            if (o1.equals(o2))
                return 0;

            return Utils.compareNotNullSigned(getBytesNoCopy(), v.getBytesNoCopy());
        }

        return h1 > h2 ? 1 : -1;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return getObject().hashCode();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object other) {
        if (!(other instanceof Value))
            return false;

        Value otherVal = (Value)other;

        return otherVal.getType() == Value.JAVA_OBJECT
            && getObject().equals(otherVal.getObject());
    }

    /** {@inheritDoc} */
    @Override public Value convertPrecision(long precision, boolean force) {
        return this;
    }

    /** {@inheritDoc} */
    @Override public int getMemory() {
        return 0;
    }
}

