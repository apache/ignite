/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import org.h2.engine.SysProperties;
import org.h2.store.DataHandler;
import org.h2.util.Bits;
import org.h2.util.JdbcUtils;
import org.h2.util.Utils;

/**
 * Implementation of the OBJECT data type.
 */
public class ValueJavaObject extends ValueBytes {

    private static final ValueJavaObject EMPTY =
            new ValueJavaObject(Utils.EMPTY_BYTES, null);
    private final DataHandler dataHandler;

    protected ValueJavaObject(byte[] v, DataHandler dataHandler) {
        super(v);
        this.dataHandler = dataHandler;
    }

    /**
     * Get or create a java object value for the given byte array.
     * Do not clone the data.
     *
     * @param javaObject the object
     * @param b the byte array
     * @param dataHandler provides the object serializer
     * @return the value
     */
    public static ValueJavaObject getNoCopy(Object javaObject, byte[] b,
            DataHandler dataHandler) {
        if (b != null && b.length == 0) {
            return EMPTY;
        }
        ValueJavaObject obj;
        if (SysProperties.serializeJavaObject) {
            if (b == null) {
                b = JdbcUtils.serialize(javaObject, dataHandler);
            }
            obj = new ValueJavaObject(b, dataHandler);
        } else {
            obj = new NotSerialized(javaObject, b, dataHandler);
        }
        if (b == null || b.length > SysProperties.OBJECT_CACHE_MAX_PER_ELEMENT_SIZE) {
            return obj;
        }
        return (ValueJavaObject) Value.cache(obj);
    }

    @Override
    public int getType() {
        return Value.JAVA_OBJECT;
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex)
            throws SQLException {
        Object obj = JdbcUtils.deserialize(getBytesNoCopy(), getDataHandler());
        prep.setObject(parameterIndex, obj, Types.JAVA_OBJECT);
    }

    /**
     * Value which serializes java object only for I/O operations.
     * Used when property {@link SysProperties#serializeJavaObject} is disabled.
     *
     * @author Sergi Vladykin
     */
    private static class NotSerialized extends ValueJavaObject {

        private Object javaObject;

        private int displaySize = -1;

        NotSerialized(Object javaObject, byte[] v, DataHandler dataHandler) {
            super(v, dataHandler);
            this.javaObject = javaObject;
        }

        @Override
        public void set(PreparedStatement prep, int parameterIndex)
                throws SQLException {
            prep.setObject(parameterIndex, getObject(), Types.JAVA_OBJECT);
        }

        @Override
        public byte[] getBytesNoCopy() {
            if (value == null) {
                value = JdbcUtils.serialize(javaObject, null);
            }
            return value;
        }

        @Override
        protected int compareSecure(Value v, CompareMode mode) {
            Object o1 = getObject();
            Object o2 = v.getObject();

            boolean o1Comparable = o1 instanceof Comparable;
            boolean o2Comparable = o2 instanceof Comparable;

            if (o1Comparable && o2Comparable &&
                    Utils.haveCommonComparableSuperclass(o1.getClass(), o2.getClass())) {
                @SuppressWarnings("unchecked")
                Comparable<Object> c1 = (Comparable<Object>) o1;
                return c1.compareTo(o2);
            }

            // group by types
            if (o1.getClass() != o2.getClass()) {
                if (o1Comparable != o2Comparable) {
                    return o1Comparable ? -1 : 1;
                }
                return o1.getClass().getName().compareTo(o2.getClass().getName());
            }

            // compare hash codes
            int h1 = hashCode();
            int h2 = v.hashCode();

            if (h1 == h2) {
                if (o1.equals(o2)) {
                    return 0;
                }
                return Bits.compareNotNullSigned(getBytesNoCopy(), v.getBytesNoCopy());
            }

            return h1 > h2 ? 1 : -1;
        }

        @Override
        public String getString() {
            String str = getObject().toString();
            if (displaySize == -1) {
                displaySize = str.length();
            }
            return str;
        }

        @Override
        public long getPrecision() {
            return 0;
        }

        @Override
        public int hashCode() {
            if (hash == 0) {
                hash = getObject().hashCode();
            }
            return hash;
        }

        @Override
        public Object getObject() {
            if (javaObject == null) {
                javaObject = JdbcUtils.deserialize(value, getDataHandler());
            }
            return javaObject;
        }

        @Override
        public int getDisplaySize() {
            if (displaySize == -1) {
                displaySize = getString().length();
            }
            return displaySize;
        }

        @Override
        public int getMemory() {
            if (value == null) {
                return DataType.getDataType(getType()).memory;
            }
            int mem = super.getMemory();
            if (javaObject != null) {
                mem *= 2;
            }
            return mem;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof NotSerialized)) {
                return false;
            }
            return getObject().equals(((NotSerialized) other).getObject());
        }

        @Override
        public Value convertPrecision(long precision, boolean force) {
            return this;
        }
    }

    @Override
    protected DataHandler getDataHandler() {
        return dataHandler;
    }
}
