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

import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.*;
import sun.misc.*;

import java.io.*;
import java.lang.reflect.*;
import java.util.*;

import static java.lang.reflect.Modifier.*;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.*;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.ENUM;

/**
 * Class descriptor.
 */
class OptimizedClassDescriptor {
    /** Unsafe. */
    private static final Unsafe UNSAFE = GridUnsafe.unsafe();

    /** Class. */
    private final Class<?> cls;

    /** Context. */
    private final MarshallerContext ctx;

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

    /**
     * Creates descriptor for class.
     *
     * @param cls Class.
     * @param ctx Context.
     * @param mapper ID mapper.
     * @throws IOException In case of error.
     */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    OptimizedClassDescriptor(Class<?> cls, MarshallerContext ctx, OptimizedMarshallerIdMapper mapper)
        throws IOException {
        this.cls = cls;
        this.ctx = ctx;
        this.mapper = mapper;

        name = cls.getName();

        int typeId;

        if (mapper != null) {
            typeId = mapper.typeId(name);

            if (typeId == 0)
                typeId = name.hashCode();
        }
        else
            typeId = name.hashCode();

        this.typeId = typeId;

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
                    dfltsFieldOff = UNSAFE.objectFieldOffset(Properties.class.getDeclaredField("defaults"));
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
                    loadFactorFieldOff = UNSAFE.objectFieldOffset(HashMap.class.getDeclaredField("loadFactor"));
                }
                catch (NoSuchFieldException e) {
                    throw new IOException(e);
                }
            }
            else if (cls == HashSet.class) {
                type = HASH_SET;

                try {
                    loadFactorFieldOff = UNSAFE.objectFieldOffset(HashMap.class.getDeclaredField("loadFactor"));
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
                    loadFactorFieldOff = UNSAFE.objectFieldOffset(HashMap.class.getDeclaredField("loadFactor"));
                    accessOrderFieldOff = UNSAFE.objectFieldOffset(LinkedHashMap.class.getDeclaredField("accessOrder"));
                }
                catch (NoSuchFieldException e) {
                    throw new IOException(e);
                }
            }
            else if (cls == LinkedHashSet.class) {
                type = LINKED_HASH_SET;

                try {
                    loadFactorFieldOff = UNSAFE.objectFieldOffset(HashMap.class.getDeclaredField("loadFactor"));
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
                    List<List<Field>> fields = new ArrayList<>();
                    List<List<T2<OptimizedFieldType, Long>>> fieldOffs = new ArrayList<>();
                    List<Map<String, IgniteBiTuple<Integer, OptimizedFieldType>>> fieldInfoMaps = new ArrayList<>();
                    List<List<IgniteBiTuple<Integer, OptimizedFieldType>>> fieldInfoLists = new ArrayList<>();

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

                        Field[] clsFields0 = c.getDeclaredFields();

                        Arrays.sort(clsFields0, new Comparator<Field>() {
                            @Override public int compare(Field f1, Field f2) {
                                return f1.getName().compareTo(f2.getName());
                            }
                        });

                        List<Field> clsFields = new ArrayList<>(clsFields0.length);
                        List<T2<OptimizedFieldType, Long>> clsFieldOffs =
                            new ArrayList<>(clsFields0.length);

                        for (int i = 0; i < clsFields0.length; i++) {
                            Field f = clsFields0[i];

                            int mod = f.getModifiers();

                            if (!isStatic(mod) && !isTransient(mod)) {
                                OptimizedFieldType type = fieldType(f.getType());

                                clsFields.add(f);
                                clsFieldOffs.add(new T2<>(type, UNSAFE.objectFieldOffset(f)));
                            }
                        }

                        fields.add(clsFields);
                        fieldOffs.add(clsFieldOffs);

                        Map<String, IgniteBiTuple<Integer, OptimizedFieldType>> fieldInfoMap = null;

                        try {
                            Field serFieldsDesc = c.getDeclaredField("serialPersistentFields");

                            int mod = serFieldsDesc.getModifiers();

                            if (serFieldsDesc.getType() == ObjectStreamField[].class &&
                                isPrivate(mod) && isStatic(mod) && isFinal(mod)) {
                                serFieldsDesc.setAccessible(true);

                                ObjectStreamField[] serFields = (ObjectStreamField[])serFieldsDesc.get(null);

                                fieldInfoMap = new HashMap<>();

                                for (int i = 0; i < serFields.length; i++) {
                                    ObjectStreamField serField = serFields[i];

                                    fieldInfoMap.put(serField.getName(), F.t(i, fieldType(serField.getType())));
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

                        if (fieldInfoMap == null) {
                            fieldInfoMap = new HashMap<>();

                            for (int i = 0; i < clsFields.size(); i++) {
                                Field f = clsFields.get(i);

                                fieldInfoMap.put(f.getName(), F.t(i, fieldType(f.getType())));
                            }
                        }

                        fieldInfoMaps.add(fieldInfoMap);

                        List<IgniteBiTuple<Integer, OptimizedFieldType>> fieldInfoList =
                            new ArrayList<>(fieldInfoMap.values());

                        Collections.sort(fieldInfoList, new Comparator<IgniteBiTuple<Integer, OptimizedFieldType>>() {
                            @Override public int compare(IgniteBiTuple<Integer, OptimizedFieldType> t1,
                                IgniteBiTuple<Integer, OptimizedFieldType> t2) {
                                return t1.get1().compareTo(t2.get1());
                            }
                        });

                        fieldInfoLists.add(fieldInfoList);
                    }

                    Collections.reverse(writeObjMtds);
                    Collections.reverse(readObjMtds);
                    Collections.reverse(fields);
                    Collections.reverse(fieldOffs);
                    Collections.reverse(fieldInfoMaps);
                    Collections.reverse(fieldInfoLists);

                    this.fields = new Fields(fields, fieldOffs, fieldInfoLists, fieldInfoMaps);
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
     * @return Type ID.
     */
    int typeId() {
        return typeId;
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
                out.writeUTF(obj.getClass().getComponentType().getName());
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
                OptimizedClassDescriptor desc = classDescriptor((Class<?>)obj, ctx, mapper);

                out.writeInt(desc.typeId());

                break;

            case ENUM:
                out.writeInt(typeId);
                out.writeInt(((Enum)obj).ordinal());

                break;

            case EXTERNALIZABLE:
                out.writeInt(typeId);
                out.writeShort(checksum);
                out.writeExternalizable(obj);

                break;

            case SERIALIZABLE:
                if (out.requireSerializable() && !isSerial)
                    throw new NotSerializableException("Must implement java.io.Serializable or " +
                        "set OptimizedMarshaller.setRequireSerializable() to false " +
                        "(note that performance may degrade if object is not Serializable): " + name);

                out.writeInt(typeId);
                out.writeShort(checksum);
                out.writeSerializable(obj, writeObjMtds, fields);

                break;

            default:
                throw new IllegalStateException("Invalid class type: " + type);
        }
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
     * Encapsulates data about class fields.
     */
    @SuppressWarnings("PackageVisibleInnerClass")
    static class Fields {
        /** Fields. */
        private final List<List<Field>> fields;

        /** Fields offsets. */
        private final List<List<T2<OptimizedFieldType, Long>>> fieldOffs;

        /** Fields details lists. */
        private final List<List<IgniteBiTuple<Integer, OptimizedFieldType>>> fieldInfoLists;

        /** Fields details maps. */
        private final List<Map<String, IgniteBiTuple<Integer, OptimizedFieldType>>> fieldInfoMaps;

        /**
         * Creates new instance.
         *
         * @param fields Fields.
         * @param fieldOffs Field offsets.
         * @param fieldInfoLists List of field details sequences for each type in the object's class hierarchy.
         * @param fieldInfoMaps List of field details maps for each type in the object's class hierarchy.
         */
        Fields(List<List<Field>> fields, List<List<T2<OptimizedFieldType, Long>>> fieldOffs,
            List<List<IgniteBiTuple<Integer, OptimizedFieldType>>> fieldInfoLists,
            List<Map<String, IgniteBiTuple<Integer, OptimizedFieldType>>> fieldInfoMaps) {
            this.fields = fields;
            this.fieldOffs = fieldOffs;
            this.fieldInfoLists = fieldInfoLists;
            this.fieldInfoMaps = fieldInfoMaps;
        }

        /**
         * Returns class's own fields (excluding inherited).
         *
         * @return List of fields or {@code null} if fields list is empty.
         */
        List<Field> ownFields() {
            return fields.isEmpty() ? null : fields.get(fields.size() - 1);
        }

        /**
         * Returns field types and their offsets.
         *
         * @param i hierarchy level where 0 corresponds to top level.
         * @return list of pairs where first value is field type and second value is its offset.
         */
        List<T2<OptimizedFieldType, Long>> fieldOffs(int i) {
            return fieldOffs.get(i);
        }

        /**
         * Returns field sequence numbers and their types as list.
         *
         * @param i hierarchy level where 0 corresponds to top level.
         * @return list of pairs (field number, field type) for the given hierarchy level.
         */
        List<IgniteBiTuple<Integer, OptimizedFieldType>> fieldInfoList(int i) {
            return fieldInfoLists.get(i);
        }

        /**
         * Returns field sequence numbers and their types as map where key is a field name,
         *
         * @param i hierarchy level where 0 corresponds to top level.
         * @return map of field names and their details.
         */
        Map<String, IgniteBiTuple<Integer, OptimizedFieldType>> fieldInfoMap(int i) {
            return fieldInfoMaps.get(i);
        }
    }
}
