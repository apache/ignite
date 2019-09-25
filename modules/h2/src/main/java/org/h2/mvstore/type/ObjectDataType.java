/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.type;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.UUID;
import org.h2.mvstore.DataUtils;
import org.h2.mvstore.WriteBuffer;
import org.h2.util.Utils;

/**
 * A data type implementation for the most common data types, including
 * serializable objects.
 */
public class ObjectDataType implements DataType {

    /**
     * The type constants are also used as tag values.
     */
    static final int TYPE_NULL = 0;
    static final int TYPE_BOOLEAN = 1;
    static final int TYPE_BYTE = 2;
    static final int TYPE_SHORT = 3;
    static final int TYPE_INT = 4;
    static final int TYPE_LONG = 5;
    static final int TYPE_BIG_INTEGER = 6;
    static final int TYPE_FLOAT = 7;
    static final int TYPE_DOUBLE = 8;
    static final int TYPE_BIG_DECIMAL = 9;
    static final int TYPE_CHAR = 10;
    static final int TYPE_STRING = 11;
    static final int TYPE_UUID = 12;
    static final int TYPE_DATE = 13;
    static final int TYPE_ARRAY = 14;
    static final int TYPE_SERIALIZED_OBJECT = 19;

    /**
     * For very common values (e.g. 0 and 1) we save space by encoding the value
     * in the tag. e.g. TAG_BOOLEAN_TRUE and TAG_FLOAT_0.
     */
    static final int TAG_BOOLEAN_TRUE = 32;
    static final int TAG_INTEGER_NEGATIVE = 33;
    static final int TAG_INTEGER_FIXED = 34;
    static final int TAG_LONG_NEGATIVE = 35;
    static final int TAG_LONG_FIXED = 36;
    static final int TAG_BIG_INTEGER_0 = 37;
    static final int TAG_BIG_INTEGER_1 = 38;
    static final int TAG_BIG_INTEGER_SMALL = 39;
    static final int TAG_FLOAT_0 = 40;
    static final int TAG_FLOAT_1 = 41;
    static final int TAG_FLOAT_FIXED = 42;
    static final int TAG_DOUBLE_0 = 43;
    static final int TAG_DOUBLE_1 = 44;
    static final int TAG_DOUBLE_FIXED = 45;
    static final int TAG_BIG_DECIMAL_0 = 46;
    static final int TAG_BIG_DECIMAL_1 = 47;
    static final int TAG_BIG_DECIMAL_SMALL = 48;
    static final int TAG_BIG_DECIMAL_SMALL_SCALED = 49;

    /**
     * For small-values/small-arrays, we encode the value/array-length in the
     * tag.
     */
    static final int TAG_INTEGER_0_15 = 64;
    static final int TAG_LONG_0_7 = 80;
    static final int TAG_STRING_0_15 = 88;
    static final int TAG_BYTE_ARRAY_0_15 = 104;

    /**
     * Constants for floating point synchronization.
     */
    static final int FLOAT_ZERO_BITS = Float.floatToIntBits(0.0f);
    static final int FLOAT_ONE_BITS = Float.floatToIntBits(1.0f);
    static final long DOUBLE_ZERO_BITS = Double.doubleToLongBits(0.0d);
    static final long DOUBLE_ONE_BITS = Double.doubleToLongBits(1.0d);

    static final Class<?>[] COMMON_CLASSES = { boolean.class, byte.class,
            short.class, char.class, int.class, long.class, float.class,
            double.class, Object.class, Boolean.class, Byte.class, Short.class,
            Character.class, Integer.class, Long.class, BigInteger.class,
            Float.class, Double.class, BigDecimal.class, String.class,
            UUID.class, Date.class };

    private static final HashMap<Class<?>, Integer> COMMON_CLASSES_MAP = new HashMap<>(COMMON_CLASSES.length);

    private AutoDetectDataType last = new StringType(this);

    @Override
    public int compare(Object a, Object b) {
        return last.compare(a, b);
    }

    @Override
    public int getMemory(Object obj) {
        return last.getMemory(obj);
    }

    @Override
    public void read(ByteBuffer buff, Object[] obj, int len, boolean key) {
        for (int i = 0; i < len; i++) {
            obj[i] = read(buff);
        }
    }

    @Override
    public void write(WriteBuffer buff, Object[] obj, int len, boolean key) {
        for (int i = 0; i < len; i++) {
            write(buff, obj[i]);
        }
    }

    @Override
    public void write(WriteBuffer buff, Object obj) {
        last.write(buff, obj);
    }

    private AutoDetectDataType newType(int typeId) {
        switch (typeId) {
        case TYPE_NULL:
            return new NullType(this);
        case TYPE_BOOLEAN:
            return new BooleanType(this);
        case TYPE_BYTE:
            return new ByteType(this);
        case TYPE_SHORT:
            return new ShortType(this);
        case TYPE_CHAR:
            return new CharacterType(this);
        case TYPE_INT:
            return new IntegerType(this);
        case TYPE_LONG:
            return new LongType(this);
        case TYPE_FLOAT:
            return new FloatType(this);
        case TYPE_DOUBLE:
            return new DoubleType(this);
        case TYPE_BIG_INTEGER:
            return new BigIntegerType(this);
        case TYPE_BIG_DECIMAL:
            return new BigDecimalType(this);
        case TYPE_STRING:
            return new StringType(this);
        case TYPE_UUID:
            return new UUIDType(this);
        case TYPE_DATE:
            return new DateType(this);
        case TYPE_ARRAY:
            return new ObjectArrayType(this);
        case TYPE_SERIALIZED_OBJECT:
            return new SerializedObjectType(this);
        }
        throw DataUtils.newIllegalStateException(DataUtils.ERROR_INTERNAL,
                "Unsupported type {0}", typeId);
    }

    @Override
    public Object read(ByteBuffer buff) {
        int tag = buff.get();
        int typeId;
        if (tag <= TYPE_SERIALIZED_OBJECT) {
            typeId = tag;
        } else {
            switch (tag) {
            case TAG_BOOLEAN_TRUE:
                typeId = TYPE_BOOLEAN;
                break;
            case TAG_INTEGER_NEGATIVE:
            case TAG_INTEGER_FIXED:
                typeId = TYPE_INT;
                break;
            case TAG_LONG_NEGATIVE:
            case TAG_LONG_FIXED:
                typeId = TYPE_LONG;
                break;
            case TAG_BIG_INTEGER_0:
            case TAG_BIG_INTEGER_1:
            case TAG_BIG_INTEGER_SMALL:
                typeId = TYPE_BIG_INTEGER;
                break;
            case TAG_FLOAT_0:
            case TAG_FLOAT_1:
            case TAG_FLOAT_FIXED:
                typeId = TYPE_FLOAT;
                break;
            case TAG_DOUBLE_0:
            case TAG_DOUBLE_1:
            case TAG_DOUBLE_FIXED:
                typeId = TYPE_DOUBLE;
                break;
            case TAG_BIG_DECIMAL_0:
            case TAG_BIG_DECIMAL_1:
            case TAG_BIG_DECIMAL_SMALL:
            case TAG_BIG_DECIMAL_SMALL_SCALED:
                typeId = TYPE_BIG_DECIMAL;
                break;
            default:
                if (tag >= TAG_INTEGER_0_15 && tag <= TAG_INTEGER_0_15 + 15) {
                    typeId = TYPE_INT;
                } else if (tag >= TAG_STRING_0_15
                        && tag <= TAG_STRING_0_15 + 15) {
                    typeId = TYPE_STRING;
                } else if (tag >= TAG_LONG_0_7 && tag <= TAG_LONG_0_7 + 7) {
                    typeId = TYPE_LONG;
                } else if (tag >= TAG_BYTE_ARRAY_0_15
                        && tag <= TAG_BYTE_ARRAY_0_15 + 15) {
                    typeId = TYPE_ARRAY;
                } else {
                    throw DataUtils.newIllegalStateException(
                            DataUtils.ERROR_FILE_CORRUPT, "Unknown tag {0}",
                            tag);
                }
            }
        }
        AutoDetectDataType t = last;
        if (typeId != t.typeId) {
            last = t = newType(typeId);
        }
        return t.read(buff, tag);
    }

    private static int getTypeId(Object obj) {
        if (obj instanceof Integer) {
            return TYPE_INT;
        } else if (obj instanceof String) {
            return TYPE_STRING;
        } else if (obj instanceof Long) {
            return TYPE_LONG;
        } else if (obj instanceof Double) {
            return TYPE_DOUBLE;
        } else if (obj instanceof Float) {
            return TYPE_FLOAT;
        } else if (obj instanceof Boolean) {
            return TYPE_BOOLEAN;
        } else if (obj instanceof UUID) {
            return TYPE_UUID;
        } else if (obj instanceof Byte) {
            return TYPE_BYTE;
        } else if (obj instanceof Short) {
            return TYPE_SHORT;
        } else if (obj instanceof Character) {
            return TYPE_CHAR;
        } else if (obj == null) {
            return TYPE_NULL;
        } else if (isDate(obj)) {
            return TYPE_DATE;
        } else if (isBigInteger(obj)) {
            return TYPE_BIG_INTEGER;
        } else if (isBigDecimal(obj)) {
            return TYPE_BIG_DECIMAL;
        } else if (obj.getClass().isArray()) {
            return TYPE_ARRAY;
        }
        return TYPE_SERIALIZED_OBJECT;
    }

    /**
     * Switch the last remembered type to match the type of the given object.
     *
     * @param obj the object
     * @return the auto-detected type used
     */
    AutoDetectDataType switchType(Object obj) {
        int typeId = getTypeId(obj);
        AutoDetectDataType l = last;
        if (typeId != l.typeId) {
            last = l = newType(typeId);
        }
        return l;
    }

    /**
     * Check whether this object is a BigInteger.
     *
     * @param obj the object
     * @return true if yes
     */
    static boolean isBigInteger(Object obj) {
        return obj instanceof BigInteger && obj.getClass() == BigInteger.class;
    }

    /**
     * Check whether this object is a BigDecimal.
     *
     * @param obj the object
     * @return true if yes
     */
    static boolean isBigDecimal(Object obj) {
        return obj instanceof BigDecimal && obj.getClass() == BigDecimal.class;
    }

    /**
     * Check whether this object is a date.
     *
     * @param obj the object
     * @return true if yes
     */
    static boolean isDate(Object obj) {
        return obj instanceof Date && obj.getClass() == Date.class;
    }

    /**
     * Check whether this object is an array.
     *
     * @param obj the object
     * @return true if yes
     */
    static boolean isArray(Object obj) {
        return obj != null && obj.getClass().isArray();
    }

    /**
     * Get the class id, or null if not found.
     *
     * @param clazz the class
     * @return the class id or null
     */
    static Integer getCommonClassId(Class<?> clazz) {
        HashMap<Class<?>, Integer> map = COMMON_CLASSES_MAP;
        if (map.size() == 0) {
            // lazy initialization
            // synchronized, because the COMMON_CLASSES_MAP is not
            synchronized (map) {
                if (map.size() == 0) {
                    for (int i = 0, size = COMMON_CLASSES.length; i < size; i++) {
                        map.put(COMMON_CLASSES[i], i);
                    }
                }
            }
        }
        return map.get(clazz);
    }

    /**
     * Serialize the object to a byte array.
     *
     * @param obj the object to serialize
     * @return the byte array
     */
    public static byte[] serialize(Object obj) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream os = new ObjectOutputStream(out);
            os.writeObject(obj);
            return out.toByteArray();
        } catch (Throwable e) {
            throw DataUtils.newIllegalArgumentException(
                    "Could not serialize {0}", obj, e);
        }
    }

    /**
     * De-serialize the byte array to an object.
     *
     * @param data the byte array
     * @return the object
     */
    public static Object deserialize(byte[] data) {
        try {
            ByteArrayInputStream in = new ByteArrayInputStream(data);
            ObjectInputStream is = new ObjectInputStream(in);
            return is.readObject();
        } catch (Throwable e) {
            throw DataUtils.newIllegalArgumentException(
                    "Could not deserialize {0}", Arrays.toString(data), e);
        }
    }

    /**
     * Compare the contents of two byte arrays. If the content or length of the
     * first array is smaller than the second array, -1 is returned. If the
     * content or length of the second array is smaller than the first array, 1
     * is returned. If the contents and lengths are the same, 0 is returned.
     * <p>
     * This method interprets bytes as unsigned.
     *
     * @param data1 the first byte array (must not be null)
     * @param data2 the second byte array (must not be null)
     * @return the result of the comparison (-1, 1 or 0)
     */
    public static int compareNotNull(byte[] data1, byte[] data2) {
        if (data1 == data2) {
            return 0;
        }
        int len = Math.min(data1.length, data2.length);
        for (int i = 0; i < len; i++) {
            int b = data1[i] & 255;
            int b2 = data2[i] & 255;
            if (b != b2) {
                return b > b2 ? 1 : -1;
            }
        }
        return Integer.signum(data1.length - data2.length);
    }

    /**
     * The base class for auto-detect data types.
     */
    abstract static class AutoDetectDataType implements DataType {

        protected final ObjectDataType base;
        protected final int typeId;

        AutoDetectDataType(ObjectDataType base, int typeId) {
            this.base = base;
            this.typeId = typeId;
        }

        @Override
        public int getMemory(Object o) {
            return getType(o).getMemory(o);
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            AutoDetectDataType aType = getType(aObj);
            AutoDetectDataType bType = getType(bObj);
            int typeDiff = aType.typeId - bType.typeId;
            if (typeDiff == 0) {
                return aType.compare(aObj, bObj);
            }
            return Integer.signum(typeDiff);
        }

        @Override
        public void write(WriteBuffer buff, Object[] obj,
                int len, boolean key) {
            for (int i = 0; i < len; i++) {
                write(buff, obj[i]);
            }
        }

        @Override
        public void write(WriteBuffer buff, Object o) {
            getType(o).write(buff, o);
        }

        @Override
        public void read(ByteBuffer buff, Object[] obj,
                int len, boolean key) {
            for (int i = 0; i < len; i++) {
                obj[i] = read(buff);
            }
        }

        @Override
        public final Object read(ByteBuffer buff) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_INTERNAL,
                    "Internal error");
        }

        /**
         * Get the type for the given object.
         *
         * @param o the object
         * @return the type
         */
        AutoDetectDataType getType(Object o) {
            return base.switchType(o);
        }

        /**
         * Read an object from the buffer.
         *
         * @param buff the buffer
         * @param tag the first byte of the object (usually the type)
         * @return the read object
         */
        abstract Object read(ByteBuffer buff, int tag);

    }

    /**
     * The type for the null value
     */
    static class NullType extends AutoDetectDataType {

        NullType(ObjectDataType base) {
            super(base, TYPE_NULL);
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            if (aObj == null && bObj == null) {
                return 0;
            } else if (aObj == null) {
                return -1;
            } else if (bObj == null) {
                return 1;
            }
            return super.compare(aObj, bObj);
        }

        @Override
        public int getMemory(Object obj) {
            return obj == null ? 0 : super.getMemory(obj);
        }

        @Override
        public void write(WriteBuffer buff, Object obj) {
            if (obj != null) {
                super.write(buff, obj);
                return;
            }
            buff.put((byte) TYPE_NULL);
        }

        @Override
        public Object read(ByteBuffer buff, int tag) {
            return null;
        }

    }

    /**
     * The type for boolean true and false.
     */
    static class BooleanType extends AutoDetectDataType {

        BooleanType(ObjectDataType base) {
            super(base, TYPE_BOOLEAN);
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            if (aObj instanceof Boolean && bObj instanceof Boolean) {
                Boolean a = (Boolean) aObj;
                Boolean b = (Boolean) bObj;
                return a.compareTo(b);
            }
            return super.compare(aObj, bObj);
        }

        @Override
        public int getMemory(Object obj) {
            return obj instanceof Boolean ? 0 : super.getMemory(obj);
        }

        @Override
        public void write(WriteBuffer buff, Object obj) {
            if (!(obj instanceof Boolean)) {
                super.write(buff, obj);
                return;
            }
            int tag = ((Boolean) obj) ? TAG_BOOLEAN_TRUE : TYPE_BOOLEAN;
            buff.put((byte) tag);
        }

        @Override
        public Object read(ByteBuffer buff, int tag) {
            return tag == TYPE_BOOLEAN ? Boolean.FALSE : Boolean.TRUE;
        }

    }

    /**
     * The type for byte objects.
     */
    static class ByteType extends AutoDetectDataType {

        ByteType(ObjectDataType base) {
            super(base, TYPE_BYTE);
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            if (aObj instanceof Byte && bObj instanceof Byte) {
                Byte a = (Byte) aObj;
                Byte b = (Byte) bObj;
                return a.compareTo(b);
            }
            return super.compare(aObj, bObj);
        }

        @Override
        public int getMemory(Object obj) {
            return obj instanceof Byte ? 0 : super.getMemory(obj);
        }

        @Override
        public void write(WriteBuffer buff, Object obj) {
            if (!(obj instanceof Byte)) {
                super.write(buff, obj);
                return;
            }
            buff.put((byte) TYPE_BYTE);
            buff.put(((Byte) obj).byteValue());
        }

        @Override
        public Object read(ByteBuffer buff, int tag) {
            return buff.get();
        }

    }

    /**
     * The type for character objects.
     */
    static class CharacterType extends AutoDetectDataType {

        CharacterType(ObjectDataType base) {
            super(base, TYPE_CHAR);
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            if (aObj instanceof Character && bObj instanceof Character) {
                Character a = (Character) aObj;
                Character b = (Character) bObj;
                return a.compareTo(b);
            }
            return super.compare(aObj, bObj);
        }

        @Override
        public int getMemory(Object obj) {
            return obj instanceof Character ? 24 : super.getMemory(obj);
        }

        @Override
        public void write(WriteBuffer buff, Object obj) {
            if (!(obj instanceof Character)) {
                super.write(buff, obj);
                return;
            }
            buff.put((byte) TYPE_CHAR);
            buff.putChar(((Character) obj).charValue());
        }

        @Override
        public Object read(ByteBuffer buff, int tag) {
            return buff.getChar();
        }

    }

    /**
     * The type for short objects.
     */
    static class ShortType extends AutoDetectDataType {

        ShortType(ObjectDataType base) {
            super(base, TYPE_SHORT);
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            if (aObj instanceof Short && bObj instanceof Short) {
                Short a = (Short) aObj;
                Short b = (Short) bObj;
                return a.compareTo(b);
            }
            return super.compare(aObj, bObj);
        }

        @Override
        public int getMemory(Object obj) {
            return obj instanceof Short ? 24 : super.getMemory(obj);
        }

        @Override
        public void write(WriteBuffer buff, Object obj) {
            if (!(obj instanceof Short)) {
                super.write(buff, obj);
                return;
            }
            buff.put((byte) TYPE_SHORT);
            buff.putShort(((Short) obj).shortValue());
        }

        @Override
        public Object read(ByteBuffer buff, int tag) {
            return buff.getShort();
        }

    }

    /**
     * The type for integer objects.
     */
    static class IntegerType extends AutoDetectDataType {

        IntegerType(ObjectDataType base) {
            super(base, TYPE_INT);
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            if (aObj instanceof Integer && bObj instanceof Integer) {
                Integer a = (Integer) aObj;
                Integer b = (Integer) bObj;
                return a.compareTo(b);
            }
            return super.compare(aObj, bObj);
        }

        @Override
        public int getMemory(Object obj) {
            return obj instanceof Integer ? 24 : super.getMemory(obj);
        }

        @Override
        public void write(WriteBuffer buff, Object obj) {
            if (!(obj instanceof Integer)) {
                super.write(buff, obj);
                return;
            }
            int x = (Integer) obj;
            if (x < 0) {
                // -Integer.MIN_VALUE is smaller than 0
                if (-x < 0 || -x > DataUtils.COMPRESSED_VAR_INT_MAX) {
                    buff.put((byte) TAG_INTEGER_FIXED).putInt(x);
                } else {
                    buff.put((byte) TAG_INTEGER_NEGATIVE).putVarInt(-x);
                }
            } else if (x <= 15) {
                buff.put((byte) (TAG_INTEGER_0_15 + x));
            } else if (x <= DataUtils.COMPRESSED_VAR_INT_MAX) {
                buff.put((byte) TYPE_INT).putVarInt(x);
            } else {
                buff.put((byte) TAG_INTEGER_FIXED).putInt(x);
            }
        }

        @Override
        public Object read(ByteBuffer buff, int tag) {
            switch (tag) {
            case TYPE_INT:
                return DataUtils.readVarInt(buff);
            case TAG_INTEGER_NEGATIVE:
                return -DataUtils.readVarInt(buff);
            case TAG_INTEGER_FIXED:
                return buff.getInt();
            }
            return tag - TAG_INTEGER_0_15;
        }

    }

    /**
     * The type for long objects.
     */
    static class LongType extends AutoDetectDataType {

        LongType(ObjectDataType base) {
            super(base, TYPE_LONG);
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            if (aObj instanceof Long && bObj instanceof Long) {
                Long a = (Long) aObj;
                Long b = (Long) bObj;
                return a.compareTo(b);
            }
            return super.compare(aObj, bObj);
        }

        @Override
        public int getMemory(Object obj) {
            return obj instanceof Long ? 30 : super.getMemory(obj);
        }

        @Override
        public void write(WriteBuffer buff, Object obj) {
            if (!(obj instanceof Long)) {
                super.write(buff, obj);
                return;
            }
            long x = (Long) obj;
            if (x < 0) {
                // -Long.MIN_VALUE is smaller than 0
                if (-x < 0 || -x > DataUtils.COMPRESSED_VAR_LONG_MAX) {
                    buff.put((byte) TAG_LONG_FIXED);
                    buff.putLong(x);
                } else {
                    buff.put((byte) TAG_LONG_NEGATIVE);
                    buff.putVarLong(-x);
                }
            } else if (x <= 7) {
                buff.put((byte) (TAG_LONG_0_7 + x));
            } else if (x <= DataUtils.COMPRESSED_VAR_LONG_MAX) {
                buff.put((byte) TYPE_LONG);
                buff.putVarLong(x);
            } else {
                buff.put((byte) TAG_LONG_FIXED);
                buff.putLong(x);
            }
        }

        @Override
        public Object read(ByteBuffer buff, int tag) {
            switch (tag) {
            case TYPE_LONG:
                return DataUtils.readVarLong(buff);
            case TAG_LONG_NEGATIVE:
                return -DataUtils.readVarLong(buff);
            case TAG_LONG_FIXED:
                return buff.getLong();
            }
            return (long) (tag - TAG_LONG_0_7);
        }

    }

    /**
     * The type for float objects.
     */
    static class FloatType extends AutoDetectDataType {

        FloatType(ObjectDataType base) {
            super(base, TYPE_FLOAT);
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            if (aObj instanceof Float && bObj instanceof Float) {
                Float a = (Float) aObj;
                Float b = (Float) bObj;
                return a.compareTo(b);
            }
            return super.compare(aObj, bObj);
        }

        @Override
        public int getMemory(Object obj) {
            return obj instanceof Float ? 24 : super.getMemory(obj);
        }

        @Override
        public void write(WriteBuffer buff, Object obj) {
            if (!(obj instanceof Float)) {
                super.write(buff, obj);
                return;
            }
            float x = (Float) obj;
            int f = Float.floatToIntBits(x);
            if (f == ObjectDataType.FLOAT_ZERO_BITS) {
                buff.put((byte) TAG_FLOAT_0);
            } else if (f == ObjectDataType.FLOAT_ONE_BITS) {
                buff.put((byte) TAG_FLOAT_1);
            } else {
                int value = Integer.reverse(f);
                if (value >= 0 && value <= DataUtils.COMPRESSED_VAR_INT_MAX) {
                    buff.put((byte) TYPE_FLOAT).putVarInt(value);
                } else {
                    buff.put((byte) TAG_FLOAT_FIXED).putFloat(x);
                }
            }
        }

        @Override
        public Object read(ByteBuffer buff, int tag) {
            switch (tag) {
            case TAG_FLOAT_0:
                return 0f;
            case TAG_FLOAT_1:
                return 1f;
            case TAG_FLOAT_FIXED:
                return buff.getFloat();
            }
            return Float.intBitsToFloat(Integer.reverse(DataUtils
                    .readVarInt(buff)));
        }

    }

    /**
     * The type for double objects.
     */
    static class DoubleType extends AutoDetectDataType {

        DoubleType(ObjectDataType base) {
            super(base, TYPE_DOUBLE);
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            if (aObj instanceof Double && bObj instanceof Double) {
                Double a = (Double) aObj;
                Double b = (Double) bObj;
                return a.compareTo(b);
            }
            return super.compare(aObj, bObj);
        }

        @Override
        public int getMemory(Object obj) {
            return obj instanceof Double ? 30 : super.getMemory(obj);
        }

        @Override
        public void write(WriteBuffer buff, Object obj) {
            if (!(obj instanceof Double)) {
                super.write(buff, obj);
                return;
            }
            double x = (Double) obj;
            long d = Double.doubleToLongBits(x);
            if (d == ObjectDataType.DOUBLE_ZERO_BITS) {
                buff.put((byte) TAG_DOUBLE_0);
            } else if (d == ObjectDataType.DOUBLE_ONE_BITS) {
                buff.put((byte) TAG_DOUBLE_1);
            } else {
                long value = Long.reverse(d);
                if (value >= 0 && value <= DataUtils.COMPRESSED_VAR_LONG_MAX) {
                    buff.put((byte) TYPE_DOUBLE);
                    buff.putVarLong(value);
                } else {
                    buff.put((byte) TAG_DOUBLE_FIXED);
                    buff.putDouble(x);
                }
            }
        }

        @Override
        public Object read(ByteBuffer buff, int tag) {
            switch (tag) {
            case TAG_DOUBLE_0:
                return 0d;
            case TAG_DOUBLE_1:
                return 1d;
            case TAG_DOUBLE_FIXED:
                return buff.getDouble();
            }
            return Double.longBitsToDouble(Long.reverse(DataUtils
                    .readVarLong(buff)));
        }

    }

    /**
     * The type for BigInteger objects.
     */
    static class BigIntegerType extends AutoDetectDataType {

        BigIntegerType(ObjectDataType base) {
            super(base, TYPE_BIG_INTEGER);
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            if (isBigInteger(aObj) && isBigInteger(bObj)) {
                BigInteger a = (BigInteger) aObj;
                BigInteger b = (BigInteger) bObj;
                return a.compareTo(b);
            }
            return super.compare(aObj, bObj);
        }

        @Override
        public int getMemory(Object obj) {
            return isBigInteger(obj) ? 100 : super.getMemory(obj);
        }

        @Override
        public void write(WriteBuffer buff, Object obj) {
            if (!isBigInteger(obj)) {
                super.write(buff, obj);
                return;
            }
            BigInteger x = (BigInteger) obj;
            if (BigInteger.ZERO.equals(x)) {
                buff.put((byte) TAG_BIG_INTEGER_0);
            } else if (BigInteger.ONE.equals(x)) {
                buff.put((byte) TAG_BIG_INTEGER_1);
            } else {
                int bits = x.bitLength();
                if (bits <= 63) {
                    buff.put((byte) TAG_BIG_INTEGER_SMALL).putVarLong(
                            x.longValue());
                } else {
                    byte[] bytes = x.toByteArray();
                    buff.put((byte) TYPE_BIG_INTEGER).putVarInt(bytes.length)
                            .put(bytes);
                }
            }
        }

        @Override
        public Object read(ByteBuffer buff, int tag) {
            switch (tag) {
            case TAG_BIG_INTEGER_0:
                return BigInteger.ZERO;
            case TAG_BIG_INTEGER_1:
                return BigInteger.ONE;
            case TAG_BIG_INTEGER_SMALL:
                return BigInteger.valueOf(DataUtils.readVarLong(buff));
            }
            int len = DataUtils.readVarInt(buff);
            byte[] bytes = Utils.newBytes(len);
            buff.get(bytes);
            return new BigInteger(bytes);
        }

    }

    /**
     * The type for BigDecimal objects.
     */
    static class BigDecimalType extends AutoDetectDataType {

        BigDecimalType(ObjectDataType base) {
            super(base, TYPE_BIG_DECIMAL);
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            if (isBigDecimal(aObj) && isBigDecimal(bObj)) {
                BigDecimal a = (BigDecimal) aObj;
                BigDecimal b = (BigDecimal) bObj;
                return a.compareTo(b);
            }
            return super.compare(aObj, bObj);
        }

        @Override
        public int getMemory(Object obj) {
            return isBigDecimal(obj) ? 150 : super.getMemory(obj);
        }

        @Override
        public void write(WriteBuffer buff, Object obj) {
            if (!isBigDecimal(obj)) {
                super.write(buff, obj);
                return;
            }
            BigDecimal x = (BigDecimal) obj;
            if (BigDecimal.ZERO.equals(x)) {
                buff.put((byte) TAG_BIG_DECIMAL_0);
            } else if (BigDecimal.ONE.equals(x)) {
                buff.put((byte) TAG_BIG_DECIMAL_1);
            } else {
                int scale = x.scale();
                BigInteger b = x.unscaledValue();
                int bits = b.bitLength();
                if (bits < 64) {
                    if (scale == 0) {
                        buff.put((byte) TAG_BIG_DECIMAL_SMALL);
                    } else {
                        buff.put((byte) TAG_BIG_DECIMAL_SMALL_SCALED)
                                .putVarInt(scale);
                    }
                    buff.putVarLong(b.longValue());
                } else {
                    byte[] bytes = b.toByteArray();
                    buff.put((byte) TYPE_BIG_DECIMAL).putVarInt(scale)
                            .putVarInt(bytes.length).put(bytes);
                }
            }
        }

        @Override
        public Object read(ByteBuffer buff, int tag) {
            switch (tag) {
            case TAG_BIG_DECIMAL_0:
                return BigDecimal.ZERO;
            case TAG_BIG_DECIMAL_1:
                return BigDecimal.ONE;
            case TAG_BIG_DECIMAL_SMALL:
                return BigDecimal.valueOf(DataUtils.readVarLong(buff));
            case TAG_BIG_DECIMAL_SMALL_SCALED:
                int scale = DataUtils.readVarInt(buff);
                return BigDecimal.valueOf(DataUtils.readVarLong(buff), scale);
            }
            int scale = DataUtils.readVarInt(buff);
            int len = DataUtils.readVarInt(buff);
            byte[] bytes = Utils.newBytes(len);
            buff.get(bytes);
            BigInteger b = new BigInteger(bytes);
            return new BigDecimal(b, scale);
        }

    }

    /**
     * The type for string objects.
     */
    static class StringType extends AutoDetectDataType {

        StringType(ObjectDataType base) {
            super(base, TYPE_STRING);
        }

        @Override
        public int getMemory(Object obj) {
            if (!(obj instanceof String)) {
                return super.getMemory(obj);
            }
            return 24 + 2 * obj.toString().length();
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            if (aObj instanceof String && bObj instanceof String) {
                return aObj.toString().compareTo(bObj.toString());
            }
            return super.compare(aObj, bObj);
        }

        @Override
        public void write(WriteBuffer buff, Object obj) {
            if (!(obj instanceof String)) {
                super.write(buff, obj);
                return;
            }
            String s = (String) obj;
            int len = s.length();
            if (len <= 15) {
                buff.put((byte) (TAG_STRING_0_15 + len));
            } else {
                buff.put((byte) TYPE_STRING).putVarInt(len);
            }
            buff.putStringData(s, len);
        }

        @Override
        public Object read(ByteBuffer buff, int tag) {
            int len;
            if (tag == TYPE_STRING) {
                len = DataUtils.readVarInt(buff);
            } else {
                len = tag - TAG_STRING_0_15;
            }
            return DataUtils.readString(buff, len);
        }

    }

    /**
     * The type for UUID objects.
     */
    static class UUIDType extends AutoDetectDataType {

        UUIDType(ObjectDataType base) {
            super(base, TYPE_UUID);
        }

        @Override
        public int getMemory(Object obj) {
            return obj instanceof UUID ? 40 : super.getMemory(obj);
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            if (aObj instanceof UUID && bObj instanceof UUID) {
                UUID a = (UUID) aObj;
                UUID b = (UUID) bObj;
                return a.compareTo(b);
            }
            return super.compare(aObj, bObj);
        }

        @Override
        public void write(WriteBuffer buff, Object obj) {
            if (!(obj instanceof UUID)) {
                super.write(buff, obj);
                return;
            }
            buff.put((byte) TYPE_UUID);
            UUID a = (UUID) obj;
            buff.putLong(a.getMostSignificantBits());
            buff.putLong(a.getLeastSignificantBits());
        }

        @Override
        public Object read(ByteBuffer buff, int tag) {
            long a = buff.getLong(), b = buff.getLong();
            return new UUID(a, b);
        }

    }

    /**
     * The type for java.util.Date objects.
     */
    static class DateType extends AutoDetectDataType {

        DateType(ObjectDataType base) {
            super(base, TYPE_DATE);
        }

        @Override
        public int getMemory(Object obj) {
            return isDate(obj) ? 40 : super.getMemory(obj);
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            if (isDate(aObj) && isDate(bObj)) {
                Date a = (Date) aObj;
                Date b = (Date) bObj;
                return a.compareTo(b);
            }
            return super.compare(aObj, bObj);
        }

        @Override
        public void write(WriteBuffer buff, Object obj) {
            if (!isDate(obj)) {
                super.write(buff, obj);
                return;
            }
            buff.put((byte) TYPE_DATE);
            Date a = (Date) obj;
            buff.putLong(a.getTime());
        }

        @Override
        public Object read(ByteBuffer buff, int tag) {
            long a = buff.getLong();
            return new Date(a);
        }

    }

    /**
     * The type for object arrays.
     */
    static class ObjectArrayType extends AutoDetectDataType {

        private final ObjectDataType elementType = new ObjectDataType();

        ObjectArrayType(ObjectDataType base) {
            super(base, TYPE_ARRAY);
        }

        @Override
        public int getMemory(Object obj) {
            if (!isArray(obj)) {
                return super.getMemory(obj);
            }
            int size = 64;
            Class<?> type = obj.getClass().getComponentType();
            if (type.isPrimitive()) {
                int len = Array.getLength(obj);
                if (type == boolean.class) {
                    size += len;
                } else if (type == byte.class) {
                    size += len;
                } else if (type == char.class) {
                    size += len * 2;
                } else if (type == short.class) {
                    size += len * 2;
                } else if (type == int.class) {
                    size += len * 4;
                } else if (type == float.class) {
                    size += len * 4;
                } else if (type == double.class) {
                    size += len * 8;
                } else if (type == long.class) {
                    size += len * 8;
                }
            } else {
                for (Object x : (Object[]) obj) {
                    if (x != null) {
                        size += elementType.getMemory(x);
                    }
                }
            }
            // we say they are larger, because these objects
            // use quite a lot of disk space
            return size * 2;
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            if (!isArray(aObj) || !isArray(bObj)) {
                return super.compare(aObj, bObj);
            }
            if (aObj == bObj) {
                return 0;
            }
            Class<?> type = aObj.getClass().getComponentType();
            Class<?> bType = bObj.getClass().getComponentType();
            if (type != bType) {
                Integer classA = getCommonClassId(type);
                Integer classB = getCommonClassId(bType);
                if (classA != null) {
                    if (classB != null) {
                        return classA.compareTo(classB);
                    }
                    return -1;
                } else if (classB != null) {
                    return 1;
                }
                return type.getName().compareTo(bType.getName());
            }
            int aLen = Array.getLength(aObj);
            int bLen = Array.getLength(bObj);
            int len = Math.min(aLen, bLen);
            if (type.isPrimitive()) {
                if (type == byte.class) {
                    byte[] a = (byte[]) aObj;
                    byte[] b = (byte[]) bObj;
                    return compareNotNull(a, b);
                }
                for (int i = 0; i < len; i++) {
                    int x;
                    if (type == boolean.class) {
                        x = Integer.signum((((boolean[]) aObj)[i] ? 1 : 0)
                                - (((boolean[]) bObj)[i] ? 1 : 0));
                    } else if (type == char.class) {
                        x = Integer.signum((((char[]) aObj)[i])
                                - (((char[]) bObj)[i]));
                    } else if (type == short.class) {
                        x = Integer.signum((((short[]) aObj)[i])
                                - (((short[]) bObj)[i]));
                    } else if (type == int.class) {
                        int a = ((int[]) aObj)[i];
                        int b = ((int[]) bObj)[i];
                        x = Integer.compare(a, b);
                    } else if (type == float.class) {
                        x = Float.compare(((float[]) aObj)[i],
                                ((float[]) bObj)[i]);
                    } else if (type == double.class) {
                        x = Double.compare(((double[]) aObj)[i],
                                ((double[]) bObj)[i]);
                    } else {
                        long a = ((long[]) aObj)[i];
                        long b = ((long[]) bObj)[i];
                        x = Long.compare(a, b);
                    }
                    if (x != 0) {
                        return x;
                    }
                }
            } else {
                Object[] a = (Object[]) aObj;
                Object[] b = (Object[]) bObj;
                for (int i = 0; i < len; i++) {
                    int comp = elementType.compare(a[i], b[i]);
                    if (comp != 0) {
                        return comp;
                    }
                }
            }
            return Integer.compare(aLen, bLen);
        }

        @Override
        public void write(WriteBuffer buff, Object obj) {
            if (!isArray(obj)) {
                super.write(buff, obj);
                return;
            }
            Class<?> type = obj.getClass().getComponentType();
            Integer classId = getCommonClassId(type);
            if (classId != null) {
                if (type.isPrimitive()) {
                    if (type == byte.class) {
                        byte[] data = (byte[]) obj;
                        int len = data.length;
                        if (len <= 15) {
                            buff.put((byte) (TAG_BYTE_ARRAY_0_15 + len));
                        } else {
                            buff.put((byte) TYPE_ARRAY)
                                    .put((byte) classId.intValue())
                                    .putVarInt(len);
                        }
                        buff.put(data);
                        return;
                    }
                    int len = Array.getLength(obj);
                    buff.put((byte) TYPE_ARRAY).put((byte) classId.intValue())
                            .putVarInt(len);
                    for (int i = 0; i < len; i++) {
                        if (type == boolean.class) {
                            buff.put((byte) (((boolean[]) obj)[i] ? 1 : 0));
                        } else if (type == char.class) {
                            buff.putChar(((char[]) obj)[i]);
                        } else if (type == short.class) {
                            buff.putShort(((short[]) obj)[i]);
                        } else if (type == int.class) {
                            buff.putInt(((int[]) obj)[i]);
                        } else if (type == float.class) {
                            buff.putFloat(((float[]) obj)[i]);
                        } else if (type == double.class) {
                            buff.putDouble(((double[]) obj)[i]);
                        } else {
                            buff.putLong(((long[]) obj)[i]);
                        }
                    }
                    return;
                }
                buff.put((byte) TYPE_ARRAY).put((byte) classId.intValue());
            } else {
                buff.put((byte) TYPE_ARRAY).put((byte) -1);
                String c = type.getName();
                StringDataType.INSTANCE.write(buff, c);
            }
            Object[] array = (Object[]) obj;
            int len = array.length;
            buff.putVarInt(len);
            for (Object x : array) {
                elementType.write(buff, x);
            }
        }

        @Override
        public Object read(ByteBuffer buff, int tag) {
            if (tag != TYPE_ARRAY) {
                byte[] data;
                int len = tag - TAG_BYTE_ARRAY_0_15;
                data = Utils.newBytes(len);
                buff.get(data);
                return data;
            }
            int ct = buff.get();
            Class<?> clazz;
            Object obj;
            if (ct == -1) {
                String componentType = StringDataType.INSTANCE.read(buff);
                try {
                    clazz = Class.forName(componentType);
                } catch (Exception e) {
                    throw DataUtils.newIllegalStateException(
                            DataUtils.ERROR_SERIALIZATION,
                            "Could not get class {0}", componentType, e);
                }
            } else {
                clazz = COMMON_CLASSES[ct];
            }
            int len = DataUtils.readVarInt(buff);
            try {
                obj = Array.newInstance(clazz, len);
            } catch (Exception e) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_SERIALIZATION,
                        "Could not create array of type {0} length {1}", clazz,
                        len, e);
            }
            if (clazz.isPrimitive()) {
                for (int i = 0; i < len; i++) {
                    if (clazz == boolean.class) {
                        ((boolean[]) obj)[i] = buff.get() == 1;
                    } else if (clazz == byte.class) {
                        ((byte[]) obj)[i] = buff.get();
                    } else if (clazz == char.class) {
                        ((char[]) obj)[i] = buff.getChar();
                    } else if (clazz == short.class) {
                        ((short[]) obj)[i] = buff.getShort();
                    } else if (clazz == int.class) {
                        ((int[]) obj)[i] = buff.getInt();
                    } else if (clazz == float.class) {
                        ((float[]) obj)[i] = buff.getFloat();
                    } else if (clazz == double.class) {
                        ((double[]) obj)[i] = buff.getDouble();
                    } else {
                        ((long[]) obj)[i] = buff.getLong();
                    }
                }
            } else {
                Object[] array = (Object[]) obj;
                for (int i = 0; i < len; i++) {
                    array[i] = elementType.read(buff);
                }
            }
            return obj;
        }

    }

    /**
     * The type for serialized objects.
     */
    static class SerializedObjectType extends AutoDetectDataType {

        private int averageSize = 10_000;

        SerializedObjectType(ObjectDataType base) {
            super(base, TYPE_SERIALIZED_OBJECT);
        }

        @SuppressWarnings("unchecked")
        @Override
        public int compare(Object aObj, Object bObj) {
            if (aObj == bObj) {
                return 0;
            }
            DataType ta = getType(aObj);
            DataType tb = getType(bObj);
            if (ta != this || tb != this) {
                if (ta == tb) {
                    return ta.compare(aObj, bObj);
                }
                return super.compare(aObj, bObj);
            }
            // TODO ensure comparable type (both may be comparable but not
            // with each other)
            if (aObj instanceof Comparable) {
                if (aObj.getClass().isAssignableFrom(bObj.getClass())) {
                    return ((Comparable<Object>) aObj).compareTo(bObj);
                }
            }
            if (bObj instanceof Comparable) {
                if (bObj.getClass().isAssignableFrom(aObj.getClass())) {
                    return -((Comparable<Object>) bObj).compareTo(aObj);
                }
            }
            byte[] a = serialize(aObj);
            byte[] b = serialize(bObj);
            return compareNotNull(a, b);
        }

        @Override
        public int getMemory(Object obj) {
            DataType t = getType(obj);
            if (t == this) {
                return averageSize;
            }
            return t.getMemory(obj);
        }

        @Override
        public void write(WriteBuffer buff, Object obj) {
            DataType t = getType(obj);
            if (t != this) {
                t.write(buff, obj);
                return;
            }
            byte[] data = serialize(obj);
            // we say they are larger, because these objects
            // use quite a lot of disk space
            int size = data.length * 2;
            // adjust the average size
            // using an exponential moving average
            averageSize = (size + 15 * averageSize) / 16;
            buff.put((byte) TYPE_SERIALIZED_OBJECT).putVarInt(data.length)
                    .put(data);
        }

        @Override
        public Object read(ByteBuffer buff, int tag) {
            int len = DataUtils.readVarInt(buff);
            byte[] data = Utils.newBytes(len);
            int size = data.length * 2;
            // adjust the average size
            // using an exponential moving average
            averageSize = (size + 15 * averageSize) / 16;
            buff.get(data);
            return deserialize(data);
        }

    }

}
