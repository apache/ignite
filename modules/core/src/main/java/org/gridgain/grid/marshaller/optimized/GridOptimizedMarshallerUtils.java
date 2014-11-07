/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.marshaller.optimized;

import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;
import sun.misc.*;

import java.io.*;
import java.lang.reflect.*;
import java.nio.charset.*;
import java.security.*;
import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.marshaller.optimized.GridOptimizedMarshallable.*;

/**
 * Miscellaneous utility methods to facilitate {@link GridOptimizedMarshaller}.
 */
class GridOptimizedMarshallerUtils {
    /** Unsafe. */
    private static final Unsafe UNSAFE = GridUnsafe.unsafe();

    /** {@code Null} object reference. */
    static final byte NULL = (byte)0x70;

    /** Handle reference. */
    static final byte HANDLE = (byte)0x71;

    /** Object reference. */
    static final byte OBJECT = (byte)0x72;

    /** UTF-8 character name. */
    static final Charset UTF_8 = Charset.forName("UTF-8");

    /** Class descriptors cache. */
    private static final ConcurrentMap<Class<?>, GridOptimizedClassDescriptor> CLS_DESC_CACHE =
        new ConcurrentHashMap8<>(256);

    /** Classes cache by name. */
    private static final ConcurrentHashMap8<ClassLoader, ConcurrentHashMap8<String, Class<?>>> CLS_BY_NAME_CACHE =
        new ConcurrentHashMap8<>();

    /**
     * Suppresses default constructor, ensuring non-instantiability.
     */
    private GridOptimizedMarshallerUtils() {
        // No-op.
    }

    /**
     * Gets class for given name and class loader.
     *
     * @param name Class name.
     * @param ldr Class loader.
     * @return Class.
     * @throws ClassNotFoundException If class was not found.
     */
    static Class<?> forName(String name, ClassLoader ldr) throws ClassNotFoundException {
        assert ldr != null;
        assert name != null;

        ConcurrentHashMap8<String, Class<?>> cache = CLS_BY_NAME_CACHE.get(ldr);

        Class<?> cls = null;

        if (cache == null) {
            cache = new ConcurrentHashMap8<>();

            ConcurrentHashMap8<String, Class<?>> old = CLS_BY_NAME_CACHE.putIfAbsent(ldr, cache);

            if (old != null) {
                cache = old;

                cls = cache.get(name);
            }
        }
        else
            cls = cache.get(name);

        if (cls == null) {
            cls = Class.forName(name, true, ldr);

            cache.put(name, cls);
        }

        return cls;
    }

    /**
     * Gets descriptor for provided class.
     *
     * @param cls Class.
     * @param obj Object.
     * @return Descriptor.
     * @throws IOException In case of error.
     */
    static GridOptimizedClassDescriptor classDescriptor(Class<?> cls, @Nullable Object obj) throws IOException {
        if (obj != null) {
            if (obj instanceof GridOptimizedMarshallable) {
                GridOptimizedMarshallable m = (GridOptimizedMarshallable)obj;

                Object clsId = m.ggClassId();

                if (clsId != null && !(clsId instanceof GridOptimizedClassDescriptor))
                    throw new IOException("Method '" + obj.getClass().getName() + ".ggClassId() must return " +
                        "the value of the field '" + CLS_ID_FIELD_NAME + "'.");

                GridOptimizedClassDescriptor desc = (GridOptimizedClassDescriptor)clsId;

                if (desc == null) {
                    desc = new GridOptimizedClassDescriptor(cls);

                    try {
                        Field field = obj.getClass().getDeclaredField(CLS_ID_FIELD_NAME);

                        field.setAccessible(true);

                        Object o = field.get(null);

                        if (o == null) {
                            if ((field.getModifiers() & Modifier.STATIC) == 0)
                                throw new IOException("Field '" + CLS_ID_FIELD_NAME + "' must be declared static: " +
                                    obj.getClass().getName());

                            field.set(null, desc);

                            if (m.ggClassId() == null)
                                throw new IOException( "Method '" + obj.getClass().getName() + ".ggClassId() must " +
                                    "return the value of the field '" + CLS_ID_FIELD_NAME + "': "
                                    + obj.getClass().getName());
                        }
                        else if (!(o instanceof GridOptimizedClassDescriptor))
                            throw new IOException("Field '" + CLS_ID_FIELD_NAME + "' must be declared with " +
                                "null value: " + obj.getClass().getName());
                    }
                    catch (NoSuchFieldException e) {
                        throw new IOException("GridOptimizedMarshallable classes must have static field declared " +
                            "[fieldName=" + CLS_ID_FIELD_NAME + ", cls=" + obj.getClass().getName() + ']', e);
                    }
                    catch (IllegalAccessException e) {
                        throw new IOException("Failed to set field '" + CLS_ID_FIELD_NAME + "' on '" +
                            obj.getClass().getName() + "' class.", e);
                    }
                }

                return desc;
            }
        }

        GridOptimizedClassDescriptor desc = CLS_DESC_CACHE.get(cls);

        if (desc == null) {
            GridOptimizedClassDescriptor existing = CLS_DESC_CACHE.putIfAbsent(cls,
                desc = new GridOptimizedClassDescriptor(cls));

            if (existing != null)
                desc = existing;
        }

        return desc;
    }

    /**
     * Undeployment callback.
     *
     * @param ldr Undeployed class loader.
     */
    public static void onUndeploy(ClassLoader ldr) {
        CLS_BY_NAME_CACHE.remove(ldr);

        for (Class<?> cls : CLS_DESC_CACHE.keySet()) {
            if (ldr.equals(cls.getClassLoader()))
                CLS_DESC_CACHE.remove(cls);
        }
    }

    /**
     * Intended for test purposes only.
     */
    public static void clearCache() {
        CLS_BY_NAME_CACHE.clear();
        CLS_DESC_CACHE.clear();
    }

    /**
     *
     */
    public static void printMemoryStats() {
        X.println(">>>");
        X.println(">>> GridOptimizedMarshallerUtils memory stats:");
        X.println(" Cache size: " + CLS_DESC_CACHE.size());

        for (Map.Entry<Class<?>, GridOptimizedClassDescriptor> e : CLS_DESC_CACHE.entrySet())
            X.println(" " + e.getKey() + " : " + e.getValue());
    }

    /**
     * Computes the serial version UID value for the given class.
     * The code is taken from {@link ObjectStreamClass#computeDefaultSUID(Class)}.
     *
     * @param cls A class.
     * @param fields Fields.
     * @return A serial version UID.
     * @throws IOException If failed.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    static Long computeSerialVersionUid(Class cls, List<Field> fields) throws IOException {
        if (Serializable.class.isAssignableFrom(cls) && !Enum.class.isAssignableFrom(cls))
            return ObjectStreamClass.lookup(cls).getSerialVersionUID();

        MessageDigest md;

        try {
            md = MessageDigest.getInstance("SHA");
        }
        catch (NoSuchAlgorithmException e) {
            throw new IOException("Failed to get digest for SHA.", e);
        }

        md.update(cls.getName().getBytes(UTF_8));

        if (!F.isEmpty(fields)) {
            for (int i = 0; i < fields.size(); i++) {
                Field f = fields.get(i);

                md.update(f.getName().getBytes(UTF_8));
                md.update(f.getType().getName().getBytes(UTF_8));
            }
        }

        byte[] hashBytes = md.digest();

        long hash = 0;

        // Composes a single-long hash from the byte[] hash.
        for (int i = Math.min(hashBytes.length, 8) - 1; i >= 0; i--)
            hash = (hash << 8) | (hashBytes[i] & 0xFF);

        return hash;
    }

    /**
     * Gets byte field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @return Byte value.
     */
    static byte getByte(Object obj, long off) {
        return UNSAFE.getByte(obj, off);
    }

    /**
     * Sets byte field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @param val Value.
     */
    static void setByte(Object obj, long off, byte val) {
        UNSAFE.putByte(obj, off, val);
    }

    /**
     * Gets short field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @return Short value.
     */
    static short getShort(Object obj, long off) {
        return UNSAFE.getShort(obj, off);
    }

    /**
     * Sets short field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @param val Value.
     */
    static void setShort(Object obj, long off, short val) {
        UNSAFE.putShort(obj, off, val);
    }

    /**
     * Gets integer field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @return Integer value.
     */
    static int getInt(Object obj, long off) {
        return UNSAFE.getInt(obj, off);
    }

    /**
     * Sets integer field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @param val Value.
     */
    static void setInt(Object obj, long off, int val) {
        UNSAFE.putInt(obj, off, val);
    }

    /**
     * Gets long field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @return Long value.
     */
    static long getLong(Object obj, long off) {
        return UNSAFE.getLong(obj, off);
    }

    /**
     * Sets long field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @param val Value.
     */
    static void setLong(Object obj, long off, long val) {
        UNSAFE.putLong(obj, off, val);
    }

    /**
     * Gets float field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @return Float value.
     */
    static float getFloat(Object obj, long off) {
        return UNSAFE.getFloat(obj, off);
    }

    /**
     * Sets float field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @param val Value.
     */
    static void setFloat(Object obj, long off, float val) {
        UNSAFE.putFloat(obj, off, val);
    }

    /**
     * Gets double field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @return Double value.
     */
    static double getDouble(Object obj, long off) {
        return UNSAFE.getDouble(obj, off);
    }

    /**
     * Sets double field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @param val Value.
     */
    static void setDouble(Object obj, long off, double val) {
        UNSAFE.putDouble(obj, off, val);
    }

    /**
     * Gets char field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @return Char value.
     */
    static char getChar(Object obj, long off) {
        return UNSAFE.getChar(obj, off);
    }

    /**
     * Sets char field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @param val Value.
     */
    static void setChar(Object obj, long off, char val) {
        UNSAFE.putChar(obj, off, val);
    }

    /**
     * Gets boolean field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @return Boolean value.
     */
    static boolean getBoolean(Object obj, long off) {
        return UNSAFE.getBoolean(obj, off);
    }

    /**
     * Sets boolean field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @param val Value.
     */
    static void setBoolean(Object obj, long off, boolean val) {
        UNSAFE.putBoolean(obj, off, val);
    }

    /**
     * Gets field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @return Value.
     */
    static Object getObject(Object obj, long off) {
        return UNSAFE.getObject(obj, off);
    }

    /**
     * Sets field value.
     *
     * @param obj Object.
     * @param off Field offset.
     * @param val Value.
     */
    static void setObject(Object obj, long off, Object val) {
        UNSAFE.putObject(obj, off, val);
    }
}
