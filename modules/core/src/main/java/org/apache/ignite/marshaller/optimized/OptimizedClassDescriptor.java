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

package org.apache.ignite.marshaller.optimized;

import java.io.Externalizable;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamField;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.marshaller.MarshallerContext;
import org.apache.ignite.marshaller.MarshallerExclusions;
import org.apache.ignite.internal.util.SerializableTransient;
import org.apache.ignite.marshaller.MarshallerUtils;

import static java.lang.reflect.Modifier.isFinal;
import static java.lang.reflect.Modifier.isPrivate;
import static java.lang.reflect.Modifier.isStatic;
import static java.lang.reflect.Modifier.isTransient;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.ARRAY_LIST;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.BOOLEAN;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.BOOLEAN_ARR;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.BYTE;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.BYTE_ARR;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.CHAR;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.CHAR_ARR;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.CLS;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.DATE;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.DOUBLE;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.DOUBLE_ARR;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.ENUM;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.EXTERNALIZABLE;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.FLOAT;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.FLOAT_ARR;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.HASH_MAP;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.HASH_SET;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.HASH_SET_MAP_OFF;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.INT;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.INT_ARR;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.LINKED_HASH_MAP;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.LINKED_HASH_SET;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.LINKED_LIST;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.LONG;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.LONG_ARR;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.OBJ_ARR;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.PROPS;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.PROXY;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.SERIALIZABLE;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.SHORT;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.SHORT_ARR;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.STR;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.UUID;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.classDescriptor;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.computeSerialVersionUid;

/**
 * Class descriptor.
 */
class OptimizedClassDescriptor {
    /** Class. */
    private final Class<?> cls;

    /** Context. */
    private final MarshallerContext ctx;

    /** */
    private ConcurrentMap<Class, OptimizedClassDescriptor> clsMap;

    /** ID mapper. */
    private final OptimizedMarshallerIdMapper mapper;

    /** Class name. */
    private final String name;

    /** Type ID. */
    private final int typeId;

    /** Short ID. */
    private final short checksum;

    /** Class type. */
    private int type;

    /** Primitive flag. */
    private boolean isPrimitive;

    /** Enum flag. */
    private boolean isEnum;

    /** Serializable flag. */
    private boolean isSerial;

    /** Excluded flag. */
    private boolean excluded;

    /** {@code True} if descriptor is for {@link Class}. */
    private boolean isCls;

    /** Enumeration values. */
    private Object[] enumVals;

    /** Constructor. */
    private Constructor<?> constructor;

    /** Fields. */
    private Fields fields;

    /** {@code writeObject} methods. */
    private List<Method> writeObjMtds;

    /** {@code writeReplace} method. */
    private Method writeReplaceMtd;

    /** {@code readObject} methods. */
    private List<Method> readObjMtds;

    /** {@code readResolve} method. */
    private Method readResolveMtd;

    /** Defaults field offset. */
    private long dfltsFieldOff;

    /** Load factor field offset. */
    private long loadFactorFieldOff;

    /** Access order field offset. */
    private long accessOrderFieldOff;

    /** Proxy interfaces. */
    private Class<?>[] proxyIntfs;

    /** Method returns serializable transient fields. */
    private Method serTransMtd;

    /**
     * Creates descriptor for class.
     *
     * @param typeId Type ID.
     * @param clsMap Class descriptors by class map.
     * @param cls Class.
     * @param ctx Context.
     * @param mapper ID mapper.
     * @throws IOException In case of error.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    OptimizedClassDescriptor(Class<?> cls,
        int typeId,
        ConcurrentMap<Class, OptimizedClassDescriptor> clsMap,
        MarshallerContext ctx,
        OptimizedMarshallerIdMapper mapper)
        throws IOException {
        this.cls = cls;
        this.typeId = typeId;
        this.clsMap = clsMap;
        this.ctx = ctx;
        this.mapper = mapper;

        name = cls.getName();

        excluded = MarshallerExclusions.isExcluded(cls);

        if (!excluded) {
            Class<?> parent;

            if (cls == byte.class || cls == Byte.class) {
                type = BYTE;

                isPrimitive = true;
            }
            else if (cls == short.class || cls == Short.class) {
                type = SHORT;

                isPrimitive = true;
            }
            else if (cls == int.class || cls == Integer.class) {
                type = INT;

                isPrimitive = true;
            }
            else if (cls == long.class || cls == Long.class) {
                type = LONG;

                isPrimitive = true;
            }
            else if (cls == float.class || cls == Float.class) {
                type = FLOAT;

                isPrimitive = true;
            }
            else if (cls == double.class || cls == Double.class) {
                type = DOUBLE;

                isPrimitive = true;
            }
            else if (cls == char.class || cls == Character.class) {
                type = CHAR;

                isPrimitive = true;
            }
            else if (cls == boolean.class || cls == Boolean.class) {
                type = BOOLEAN;

                isPrimitive = true;
            }
            else if (cls == byte[].class)
                type = BYTE_ARR;
            else if (cls == short[].class)
                type = SHORT_ARR;
            else if (cls == int[].class)
                type = INT_ARR;
            else if (cls == long[].class)
                type = LONG_ARR;
            else if (cls == float[].class)
                type = FLOAT_ARR;
            else if (cls == double[].class)
                type = DOUBLE_ARR;
            else if (cls == char[].class)
                type = CHAR_ARR;
            else if (cls == boolean[].class)
                type = BOOLEAN_ARR;
            else if (cls.isArray())
                type = OBJ_ARR;
            else if (cls == String.class)
                type = STR;
            else if (cls.isEnum()) {
                type = ENUM;

                isEnum = true;
                enumVals = cls.getEnumConstants();
            }
            // Support for enum constants, based on anonymous children classes.
            else if ((parent = cls.getSuperclass()) != null && parent.isEnum()) {
                type = ENUM;

                isEnum = true;
                enumVals = parent.getEnumConstants();
            }
            else if (cls == UUID.class)
                type = UUID;
            else if (cls == Properties.class) {
                type = PROPS;

                try {
                    dfltsFieldOff = GridUnsafe.objectFieldOffset(Properties.class.getDeclaredField("defaults"));
                }
                catch (NoSuchFieldException e) {
                    throw new IOException(e);
                }
            }
            else if (cls == ArrayList.class)
                type = ARRAY_LIST;
            else if (cls == HashMap.class) {
                type = HASH_MAP;

                try {
                    loadFactorFieldOff = GridUnsafe.objectFieldOffset(HashMap.class.getDeclaredField("loadFactor"));
                }
                catch (NoSuchFieldException e) {
                    throw new IOException(e);
                }
            }
            else if (cls == HashSet.class) {
                type = HASH_SET;

                try {
                    loadFactorFieldOff = GridUnsafe.objectFieldOffset(HashMap.class.getDeclaredField("loadFactor"));
                }
                catch (NoSuchFieldException e) {
                    throw new IOException(e);
                }
            }
            else if (cls == LinkedList.class)
                type = LINKED_LIST;
            else if (cls == LinkedHashMap.class) {
                type = LINKED_HASH_MAP;

                try {
                    loadFactorFieldOff =
                        GridUnsafe.objectFieldOffset(HashMap.class.getDeclaredField("loadFactor"));
                    accessOrderFieldOff =
                        GridUnsafe.objectFieldOffset(LinkedHashMap.class.getDeclaredField("accessOrder"));
                }
                catch (NoSuchFieldException e) {
                    throw new IOException(e);
                }
            }
            else if (cls == LinkedHashSet.class) {
                type = LINKED_HASH_SET;

                try {
                    loadFactorFieldOff = GridUnsafe.objectFieldOffset(HashMap.class.getDeclaredField("loadFactor"));
                }
                catch (NoSuchFieldException e) {
                    throw new IOException(e);
                }
            }
            else if (cls == Date.class)
                type = DATE;
            else if (cls == Class.class) {
                type = CLS;

                isCls = true;
            }
            else if (Proxy.class.isAssignableFrom(cls)) {
                type = PROXY;

                proxyIntfs = cls.getInterfaces();
            }
            else {
                Class<?> c = cls;

                while ((writeReplaceMtd == null || readResolveMtd == null) && c != null && !c.equals(Object.class)) {
                    if (writeReplaceMtd == null) {
                        try {
                            writeReplaceMtd = c.getDeclaredMethod("writeReplace");

                            if (!isStatic(writeReplaceMtd.getModifiers()) &&
                                !(isPrivate(writeReplaceMtd.getModifiers()) && c != cls) &&
                                writeReplaceMtd.getReturnType().equals(Object.class))
                                writeReplaceMtd.setAccessible(true);
                            else
                                // Set method back to null if it has incorrect signature.
                                writeReplaceMtd = null;
                        }
                        catch (NoSuchMethodException ignored) {
                            // No-op.
                        }
                    }

                    if (readResolveMtd == null) {
                        try {
                            readResolveMtd = c.getDeclaredMethod("readResolve");

                            if (!isStatic(readResolveMtd.getModifiers()) &&
                                !(isPrivate(readResolveMtd.getModifiers()) && c != cls) &&
                                readResolveMtd.getReturnType().equals(Object.class))
                                readResolveMtd.setAccessible(true);
                            else
                                // Set method back to null if it has incorrect signature.
                                readResolveMtd = null;
                        }
                        catch (NoSuchMethodException ignored) {
                            // No-op.
                        }
                    }

                    c = c.getSuperclass();
                }

                if (Externalizable.class.isAssignableFrom(cls)) {
                    type = EXTERNALIZABLE;

                    try {
                        constructor = !Modifier.isStatic(cls.getModifiers()) && cls.getDeclaringClass() != null ?
                            cls.getDeclaredConstructor(cls.getDeclaringClass()) :
                            cls.getDeclaredConstructor();

                        constructor.setAccessible(true);
                    }
                    catch (NoSuchMethodException e) {
                        throw new IOException("Externalizable class doesn't have default constructor: " + cls, e);
                    }
                }
                else {
                    type = SERIALIZABLE;

                    isSerial = Serializable.class.isAssignableFrom(cls);

                    writeObjMtds = new ArrayList<>();
                    readObjMtds = new ArrayList<>();
                    List<ClassFields> fields = new ArrayList<>();

                    for (c = cls; c != null && !c.equals(Object.class); c = c.getSuperclass()) {
                        Method mtd;

                        try {
                            mtd = c.getDeclaredMethod("writeObject", ObjectOutputStream.class);

                            int mod = mtd.getModifiers();

                            if (!isStatic(mod) && isPrivate(mod) && mtd.getReturnType() == Void.TYPE)
                                mtd.setAccessible(true);
                            else
                                // Set method back to null if it has incorrect signature.
                                mtd = null;
                        }
                        catch (NoSuchMethodException ignored) {
                            mtd = null;
                        }

                        writeObjMtds.add(mtd);

                        try {
                            mtd = c.getDeclaredMethod("readObject", ObjectInputStream.class);

                            int mod = mtd.getModifiers();

                            if (!isStatic(mod) && isPrivate(mod) && mtd.getReturnType() == Void.TYPE)
                                mtd.setAccessible(true);
                            else
                                // Set method back to null if it has incorrect signature.
                                mtd = null;
                        }
                        catch (NoSuchMethodException ignored) {
                            mtd = null;
                        }

                        readObjMtds.add(mtd);

                        final SerializableTransient serTransAn = c.getAnnotation(SerializableTransient.class);

                        // Custom serialization policy for transient fields.
                        if (serTransAn != null) {
                            try {
                                serTransMtd = c.getDeclaredMethod(serTransAn.methodName(), cls, IgniteProductVersion.class);

                                int mod = serTransMtd.getModifiers();

                                if (isStatic(mod) && isPrivate(mod)
                                    && serTransMtd.getReturnType() == String[].class)
                                    serTransMtd.setAccessible(true);
                                else
                                    // Set method back to null if it has incorrect signature.
                                    serTransMtd = null;
                            }
                            catch (NoSuchMethodException ignored) {
                                serTransMtd = null;
                            }
                        }

                        Field[] clsFields0 = c.getDeclaredFields();

                        Map<String, Field> fieldNames = new HashMap<>();

                        for (Field f : clsFields0)
                            fieldNames.put(f.getName(), f);

                        List<FieldInfo> clsFields = new ArrayList<>(clsFields0.length);

                        boolean hasSerialPersistentFields  = false;

                        try {
                            Field serFieldsDesc = c.getDeclaredField("serialPersistentFields");

                            int mod = serFieldsDesc.getModifiers();

                            if (serFieldsDesc.getType() == ObjectStreamField[].class &&
                                isPrivate(mod) && isStatic(mod) && isFinal(mod)) {
                                hasSerialPersistentFields = true;

                                serFieldsDesc.setAccessible(true);

                                ObjectStreamField[] serFields = (ObjectStreamField[]) serFieldsDesc.get(null);

                                for (int i = 0; i < serFields.length; i++) {
                                    ObjectStreamField serField = serFields[i];

                                    FieldInfo fieldInfo;

                                    if (!fieldNames.containsKey(serField.getName())) {
                                        fieldInfo = new FieldInfo(null,
                                            serField.getName(),
                                            -1,
                                            fieldType(serField.getType()));
                                    }
                                    else {
                                        Field f = fieldNames.get(serField.getName());

                                        fieldInfo = new FieldInfo(f,
                                            serField.getName(),
                                            GridUnsafe.objectFieldOffset(f),
                                            fieldType(serField.getType()));
                                    }

                                    clsFields.add(fieldInfo);
                                }
                            }
                        }
                        catch (NoSuchFieldException ignored) {
                            // No-op.
                        }
                        catch (IllegalAccessException e) {
                            throw new IOException("Failed to get value of 'serialPersistentFields' field in class: " +
                                cls.getName(), e);
                        }

                        if (!hasSerialPersistentFields) {
                            for (int i = 0; i < clsFields0.length; i++) {
                                Field f = clsFields0[i];

                                int mod = f.getModifiers();

                                if (!isStatic(mod) && !isTransient(mod)) {
                                    FieldInfo fieldInfo = new FieldInfo(f, f.getName(),
                                        GridUnsafe.objectFieldOffset(f), fieldType(f.getType()));

                                    clsFields.add(fieldInfo);
                                }
                            }
                        }

                        Collections.sort(clsFields, new Comparator<FieldInfo>() {
                            @Override public int compare(FieldInfo t1, FieldInfo t2) {
                                return t1.name().compareTo(t2.name());
                            }
                        });

                        fields.add(new ClassFields(clsFields));
                    }

                    Collections.reverse(writeObjMtds);
                    Collections.reverse(readObjMtds);
                    Collections.reverse(fields);

                    this.fields = new Fields(fields);
                }
            }
        }

        checksum = computeSerialVersionUid(cls, fields != null ? fields.ownFields() : null);
    }

    /**
     * @return Excluded flag.
     */
    boolean excluded() {
        return excluded;
    }

    /**
     * @return Class.
     */
    Class<?> describedClass() {
        return cls;
    }

    /**
     * @return Primitive flag.
     */
    boolean isPrimitive() {
        return isPrimitive;
    }

    /**
     * @return Enum flag.
     */
    boolean isEnum() {
        return isEnum;
    }

    /**
     * @return {@code True} if descriptor is for {@link Class}.
     */
    boolean isClass() {
        return isCls;
    }

    /**
     * @return {@code True} if descriptor is for {@link Proxy}.
     */
    boolean isProxy() {
        return type == PROXY;
    }

    /**
     * Replaces object.
     *
     * @param obj Object.
     * @return Replaced object or {@code null} if there is no {@code writeReplace} method.
     * @throws IOException In case of error.
     */
    Object replace(Object obj) throws IOException {
        if (writeReplaceMtd != null) {
            try {
                return writeReplaceMtd.invoke(obj);
            }
            catch (IllegalAccessException | InvocationTargetException e) {
                throw new IOException(e);
            }
        }

        return obj;
    }

    /**
     * Writes object to stream.
     *
     * @param out Output stream.
     * @param obj Object.
     * @throws IOException In case of error.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    void write(OptimizedObjectOutputStream out, Object obj) throws IOException {
        out.write(type);

        switch (type) {
            case BYTE:
                out.writeByte((Byte)obj);

                break;

            case SHORT:
                out.writeShort((Short)obj);

                break;

            case INT:
                out.writeInt((Integer)obj);

                break;

            case LONG:
                out.writeLong((Long)obj);

                break;

            case FLOAT:
                out.writeFloat((Float)obj);

                break;

            case DOUBLE:
                out.writeDouble((Double)obj);

                break;

            case CHAR:
                out.writeChar((Character)obj);

                break;

            case BOOLEAN:
                out.writeBoolean((Boolean)obj);

                break;

            case BYTE_ARR:
                out.writeByteArray((byte[])obj);

                break;

            case SHORT_ARR:
                out.writeShortArray((short[])obj);

                break;

            case INT_ARR:
                out.writeIntArray((int[])obj);

                break;

            case LONG_ARR:
                out.writeLongArray((long[])obj);

                break;

            case FLOAT_ARR:
                out.writeFloatArray((float[])obj);

                break;

            case DOUBLE_ARR:
                out.writeDoubleArray((double[])obj);

                break;

            case CHAR_ARR:
                out.writeCharArray((char[])obj);

                break;

            case BOOLEAN_ARR:
                out.writeBooleanArray((boolean[])obj);

                break;

            case OBJ_ARR:
                OptimizedClassDescriptor compDesc = classDescriptor(clsMap,
                    obj.getClass().getComponentType(),
                    ctx,
                    mapper);

                compDesc.writeTypeData(out);

                out.writeArray((Object[])obj);

                break;

            case STR:
                out.writeString((String)obj);

                break;

            case UUID:
                out.writeUuid((UUID)obj);

                break;

            case PROPS:
                out.writeProperties((Properties)obj, dfltsFieldOff);

                break;

            case ARRAY_LIST:
                out.writeArrayList((ArrayList<?>)obj);

                break;

            case HASH_MAP:
                out.writeHashMap((HashMap<?, ?>)obj, loadFactorFieldOff, false);

                break;

            case HASH_SET:
                out.writeHashSet((HashSet<?>)obj, HASH_SET_MAP_OFF, loadFactorFieldOff);

                break;

            case LINKED_LIST:
                out.writeLinkedList((LinkedList<?>)obj);

                break;

            case LINKED_HASH_MAP:
                out.writeLinkedHashMap((LinkedHashMap<?, ?>)obj, loadFactorFieldOff, accessOrderFieldOff, false);

                break;

            case LINKED_HASH_SET:
                out.writeLinkedHashSet((LinkedHashSet<?>)obj, HASH_SET_MAP_OFF, loadFactorFieldOff);

                break;

            case DATE:
                out.writeDate((Date)obj);

                break;

            case CLS:
                OptimizedClassDescriptor clsDesc = classDescriptor(clsMap, (Class<?>)obj, ctx, mapper);

                clsDesc.writeTypeData(out);

                break;

            case PROXY:
                out.writeInt(proxyIntfs.length);

                for (Class<?> intf : proxyIntfs) {
                    OptimizedClassDescriptor intfDesc = classDescriptor(clsMap, intf, ctx, mapper);

                    intfDesc.writeTypeData(out);
                }

                InvocationHandler ih = Proxy.getInvocationHandler(obj);

                assert ih != null;

                out.writeObject(ih);

                break;

            case ENUM:
                writeTypeData(out);

                out.writeInt(((Enum)obj).ordinal());

                break;

            case EXTERNALIZABLE:
                writeTypeData(out);

                out.writeShort(checksum);
                out.writeExternalizable(obj);

                break;

            case SERIALIZABLE:
                if (out.requireSerializable() && !isSerial)
                    throw new NotSerializableException("Must implement java.io.Serializable or " +
                        "set OptimizedMarshaller.setRequireSerializable() to false " +
                        "(note that performance may degrade if object is not Serializable): " + name);

                writeTypeData(out);

                out.writeShort(checksum);
                out.writeSerializable(obj, writeObjMtds, serializableFields(obj.getClass(), obj, null));

                break;

            default:
                throw new IllegalStateException("Invalid class type: " + type);
        }
    }

    /**
     * Gets list of serializable fields. If {@link #serTransMtd} method
     * returns list of transient fields, they will be added to other fields.
     * Transient fields that are not included in that list will be normally
     * ignored.
     *
     * @param cls Class.
     * @param obj Object.
     * @param ver Job sender version.
     * @return Serializable fields.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    private Fields serializableFields(Class<?> cls, Object obj, IgniteProductVersion ver) {
        if (serTransMtd == null)
            return fields;

        try {
            final String[] transFields = (String[])serTransMtd.invoke(cls, obj, ver);

            if (transFields == null || transFields.length == 0)
                return fields;

            List<FieldInfo> clsFields = new ArrayList<>();

            clsFields.addAll(fields.fields.get(0).fields);

            for (int i = 0; i < transFields.length; i++) {
                final String fieldName = transFields[i];

                final Field f = cls.getDeclaredField(fieldName);

                FieldInfo fieldInfo = new FieldInfo(f, f.getName(),
                    GridUnsafe.objectFieldOffset(f), fieldType(f.getType()));

                clsFields.add(fieldInfo);
            }

            Collections.sort(clsFields, new Comparator<FieldInfo>() {
                @Override public int compare(FieldInfo t1, FieldInfo t2) {
                    return t1.name().compareTo(t2.name());
                }
            });

            List<ClassFields> fields = new ArrayList<>();

            fields.add(new ClassFields(clsFields));

            return new Fields(fields);
        }
        catch (Exception ignored) {
            return fields;
        }
    }

    /**
     * @param out Output stream.
     * @throws IOException In case of error.
     */
    void writeTypeData(OptimizedObjectOutputStream out) throws IOException {
        out.writeInt(typeId);

        if (typeId == 0)
            out.writeUTF(name);
    }

    /**
     * Reads object from stream.
     *
     * @param in Input stream.
     * @return Object.
     * @throws ClassNotFoundException If class not found.
     * @throws IOException In case of error.
     */
    Object read(OptimizedObjectInputStream in) throws ClassNotFoundException, IOException {
        switch (type) {
            case ENUM:
                return enumVals[in.readInt()];

            case EXTERNALIZABLE:
                verifyChecksum(in.readShort());

                return in.readExternalizable(constructor, readResolveMtd);

            case SERIALIZABLE:
                verifyChecksum(in.readShort());

                // If no serialize method, then unmarshal as usual.
                if (serTransMtd != null)
                    return in.readSerializable(cls, readObjMtds, readResolveMtd,
                        serializableFields(cls, null, MarshallerUtils.jobSenderVersion()));
                else
                    return in.readSerializable(cls, readObjMtds, readResolveMtd, fields);

            default:
                assert false : "Unexpected type: " + type;

                return null;
        }
    }

    /**
     * @param checksum Checksum.
     * @throws ClassNotFoundException If checksum is wrong.
     * @throws IOException In case of error.
     */
    private void verifyChecksum(short checksum) throws ClassNotFoundException, IOException {
        if (checksum != this.checksum)
            throw new ClassNotFoundException("Optimized stream class checksum mismatch " +
                "(is same version of marshalled class present on all nodes?) " +
                "[expected=" + this.checksum + ", actual=" + checksum + ", cls=" + cls + ']');
    }

    /**
     * @param cls Class.
     * @return Type.
     */
    @SuppressWarnings("IfMayBeConditional")
    private OptimizedFieldType fieldType(Class<?> cls) {
        OptimizedFieldType type;

        if (cls == byte.class)
            type = OptimizedFieldType.BYTE;
        else if (cls == short.class)
            type = OptimizedFieldType.SHORT;
        else if (cls == int.class)
            type = OptimizedFieldType.INT;
        else if (cls == long.class)
            type = OptimizedFieldType.LONG;
        else if (cls == float.class)
            type = OptimizedFieldType.FLOAT;
        else if (cls == double.class)
            type = OptimizedFieldType.DOUBLE;
        else if (cls == char.class)
            type = OptimizedFieldType.CHAR;
        else if (cls == boolean.class)
            type = OptimizedFieldType.BOOLEAN;
        else
            type = OptimizedFieldType.OTHER;

        return type;
    }

    /**
     * Information about one field.
     */
    @SuppressWarnings("PackageVisibleInnerClass")
    static class FieldInfo {
        /** Field. */
        private final Field field;

        /** Field offset. */
        private final long fieldOffs;

        /** Field type. */
        private final OptimizedFieldType fieldType;

        /** Field name. */
        private final String fieldName;

        /**
         * @param field Field.
         * @param name Field name.
         * @param offset Field offset.
         * @param type Grid optimized field type.
         */
        FieldInfo(Field field, String name, long offset, OptimizedFieldType type) {
            this.field = field;
            fieldOffs = offset;
            fieldType = type;
            fieldName = name;
        }

        /**
         * @return Returns field.
         */
        Field field() {
            return field;
        }

        /**
         * @return Offset.
         */
        long offset() {
            return fieldOffs;
        }

        /**
         * @return Type.
         */
        OptimizedFieldType type() {
            return fieldType;
        }

        /**
         * @return Name.
         */
        String name() {
            return fieldName;
        }
    }

    /**
     * Information about one class.
     */
    static class ClassFields {
        /** Fields. */
        private final List<FieldInfo> fields;

        private final Map<String, Integer> nameToIndex;

        /**
         * @param fields Field infos.
         */
        ClassFields(List<FieldInfo> fields) {
            this.fields = fields;

            nameToIndex = U.newHashMap(fields.size());

            for (int i = 0; i < fields.size(); ++i)
                nameToIndex.put(fields.get(i).name(), i);
        }

        /**
         * @return Class fields.
         */
        List<FieldInfo> fields() {
            return fields;
        }

        /**
         * @return Fields count.
         */
        int size() {
            return fields.size();
        }

        /**
         * @param i Field's index.
         * @return FieldInfo.
         */
        FieldInfo get(int i) {
            return fields.get(i);
        }

        /**
         * @param name Field's name.
         * @return Field's index.
         */
        int getIndex(String name) {
            assert nameToIndex.containsKey(name);

            return nameToIndex.get(name);
        }
    }

    /**
     * Encapsulates data about class fields.
     */
    @SuppressWarnings("PackageVisibleInnerClass")
    static class Fields {
        /** Fields. */
        private final List<ClassFields> fields;

        /** Own fields (excluding inherited). */
        private final List<Field> ownFields;

        /**
         * Creates new instance.
         *
         * @param fields Fields.
         */
        Fields(List<ClassFields> fields) {
            this.fields = fields;

            if (fields.isEmpty())
                ownFields = null;
            else {
                ownFields = new ArrayList<>(fields.size());

                for (FieldInfo f : fields.get(fields.size() - 1).fields()) {
                    if (f.field() != null)
                        ownFields.add(f.field);
                }
            }
        }

        /**
         * Returns class's own fields (excluding inherited).
         *
         * @return List of fields or {@code null} if fields list is empty.
         */
        List<Field> ownFields() {
            return ownFields;
        }

        /**
         * Returns field types and their offsets.
         *
         * @param i hierarchy level where 0 corresponds to top level.
         * @return list of pairs where first value is field type and second value is its offset.
         */
        ClassFields fields(int i) {
            return fields.get(i);
        }
    }
}
