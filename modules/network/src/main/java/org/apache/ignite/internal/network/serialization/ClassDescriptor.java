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

package org.apache.ignite.internal.network.serialization;

import static java.util.stream.Collectors.toUnmodifiableMap;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntMaps;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Class descriptor for the user object serialization.
 */
public class ClassDescriptor {
    /**
     * Class.
     */
    @NotNull
    private final Class<?> clazz;

    /**
     * Descriptor id.
     */
    private final int descriptorId;

    /**
     * Superclass descriptor (might be missing).
     */
    @Nullable
    private final ClassDescriptor superClassDescriptor;

    /**
     * List of the declared class fields' descriptors.
     */
    @NotNull
    private final List<FieldDescriptor> fields;

    /**
     * How the class is to be serialized.
     */
    private final Serialization serialization;

    /**
     * Whether the class is final.
     */
    private final boolean isFinal;

    /** Total number of bytes needed to store all primitive fields. */
    private final int primitiveFieldsDataSize;
    /** Total number of non-primitive fields. */
    private final int objectFieldsCount;

    /**
     * Size of the nulls bitmap for the described class; it is equal to the number of nullable (i.e. non-primitive)
     * fields that have a type known upfront.
     *
     * @see #fieldNullsBitmapIndices
     * @see #isRuntimeTypeKnownUpfront()
     */
    private final int fieldNullsBitmapSize;

    /**
     * Map from field names to indices in the nulls bitmap corresponding to the described class. It contains an entry
     * for each field, but only entries for nullable (that is, non-primitive) fields which types are known upfront
     * have meaningful (non-negative) indices; all other fields have -1 as a value in this map.
     *
     * @see #isRuntimeTypeKnownUpfront()
     */
    private final Object2IntMap<String> fieldNullsBitmapIndices;

    private Map<String, FieldDescriptor> fieldsByName;
    /**
     * Offsets into primitive fields data array (which has size {@link #primitiveFieldsDataSize}).
     * This array is a byte array containing data of all the primitive fields of an object.
     * (Not to be confused with the offsets used in the context of {@link sun.misc.Unsafe}).
     */
    private Object2IntMap<String> primitiveFieldDataOffsets;
    /** Indices of non-primitive fields in the object fields array. */
    private Object2IntMap<String> objectFieldIndices;

    private final SpecialSerializationMethods serializationMethods;

    /**
     * Constructor.
     */
    public ClassDescriptor(
            @NotNull Class<?> clazz,
            int descriptorId,
            @Nullable ClassDescriptor superClassDescriptor,
            @NotNull List<FieldDescriptor> fields,
            Serialization serialization
    ) {
        this.clazz = clazz;
        this.descriptorId = descriptorId;
        this.superClassDescriptor = superClassDescriptor;
        this.fields = List.copyOf(fields);
        this.serialization = serialization;
        this.isFinal = Modifier.isFinal(clazz.getModifiers());

        primitiveFieldsDataSize = computePrimitiveFieldsDataSize(fields);
        objectFieldsCount = computeObjectFieldsCount(fields);

        fieldNullsBitmapSize = computeFieldNullsBitmapSize(fields);
        fieldNullsBitmapIndices = computeFieldNullsBitmapIndices(fields);

        serializationMethods = new SpecialSerializationMethodsImpl(this);
    }

    private static int computePrimitiveFieldsDataSize(List<FieldDescriptor> fields) {
        int accumulatedBytes = 0;
        for (FieldDescriptor fieldDesc : fields) {
            if (fieldDesc.isPrimitive()) {
                accumulatedBytes += Primitives.widthInBytes(fieldDesc.clazz());
            }
        }
        return accumulatedBytes;
    }

    private static int computeObjectFieldsCount(List<FieldDescriptor> fields) {
        return (int) fields.stream()
                .filter(fieldDesc -> !fieldDesc.isPrimitive())
                .count();
    }

    private static int computeFieldNullsBitmapSize(List<FieldDescriptor> fields) {
        int count = 0;
        for (FieldDescriptor fieldDescriptor : fields) {
            if (!fieldDescriptor.isPrimitive() && fieldDescriptor.isRuntimeTypeKnownUpfront()) {
                count++;
            }
        }

        return count;
    }

    private static Object2IntMap<String> computeFieldNullsBitmapIndices(List<FieldDescriptor> fields) {
        Object2IntMap<String> map = new Object2IntOpenHashMap<>();

        int index = 0;
        for (FieldDescriptor fieldDescriptor : fields) {
            int indexToPut = !fieldDescriptor.isPrimitive() && fieldDescriptor.isRuntimeTypeKnownUpfront() ? (index++) : -1;
            map.put(fieldDescriptor.name(), indexToPut);
        }

        return Object2IntMaps.unmodifiable(map);
    }

    /**
     * Returns descriptor id.
     *
     * @return Descriptor id.
     */
    public int descriptorId() {
        return descriptorId;
    }

    /**
     * Returns descriptor of the superclass of the described class (might be {@code null}).
     *
     * @return descriptor of the superclass of the described class (might be {@code null})
     */
    @Nullable
    public ClassDescriptor superClassDescriptor() {
        return superClassDescriptor;
    }

    /**
     * Returns ID of the superclass descriptor (might be {@code null}).
     *
     * @return ID of the superclass descriptor (might be {@code null})
     */
    @Nullable
    public Integer superClassDescriptorId() {
        return superClassDescriptor == null ? null : superClassDescriptor.descriptorId();
    }

    /**
     * Returns name of the superclass (might be {@code null}).
     *
     * @return name of the superclass (might be {@code null})
     */
    @Nullable
    public String superClassName() {
        return superClassDescriptor == null ? null : superClassDescriptor.className();
    }

    /**
     * Returns declared fields' descriptors.
     *
     * @return Fields' descriptors.
     */
    @NotNull
    public List<FieldDescriptor> fields() {
        return fields;
    }

    /**
     * Returns class' name.
     *
     * @return Class' name.
     */
    @NotNull
    public String className() {
        return clazz.getName();
    }

    /**
     * Returns descriptor's class.
     *
     * @return Class.
     */
    @NotNull
    public Class<?> clazz() {
        return clazz;
    }

    /**
     * Returns serialization.
     *
     * @return Serialization.
     */
    public Serialization serialization() {
        return serialization;
    }

    /**
     * Returns serialization type.
     *
     * @return Serialization type.
     */
    public SerializationType serializationType() {
        return serialization.type();
    }

    /**
     * Returns {@code true} if class is final, {@code false} otherwise.
     *
     * @return {@code true} if class is final, {@code false} otherwise.
     */
    public boolean isFinal() {
        return isFinal;
    }

    /**
     * Returns {@code true} if the described class should be serialized as a {@link java.io.Serializable} (but not
     * using the mechanism for {@link java.io.Externalizable}).
     *
     * @return {@code true} if the described class should be serialized as a {@link java.io.Serializable}.
     */
    public boolean isSerializable() {
        return serialization.type() == SerializationType.SERIALIZABLE;
    }

    /**
     * Returns {@code true} if the described class should be serialized as an {@link java.io.Externalizable}.
     *
     * @return {@code true} if the described class should be serialized as an {@link java.io.Externalizable}.
     */
    public boolean isExternalizable() {
        return serialization.type() == SerializationType.EXTERNALIZABLE;
    }

    /**
     * Returns {@code true} if the described class is treated as a built-in.
     *
     * @return {@code true} if if the described class is treated as a built-in
     */
    public boolean isBuiltIn() {
        return serializationType() == SerializationType.BUILTIN;
    }

    /**
     * Returns {@code true} if the described class has writeObject() method.
     *
     * @return {@code true} if the described class has writeObject() method
     */
    public boolean hasWriteObject() {
        return serialization.hasWriteObject();
    }

    /**
     * Returns {@code true} if the described class has readObject() method.
     *
     * @return {@code true} if the described class has readObject() method
     */
    public boolean hasReadObject() {
        return serialization.hasReadObject();
    }

    /**
     * Returns {@code true} if the described class has readObjectNoData() method.
     *
     * @return {@code true} if the described class has readObjectNoData() method
     */
    public boolean hasReadObjectNoData() {
        return serialization.hasReadObjectNoData();
    }

    /**
     * Returns {@code true} if the described class has {@code writeReplace()} method.
     *
     * @return {@code true} if the described class has {@code writeReplace()} method
     */
    public boolean hasWriteReplace() {
        return serialization.hasWriteReplace();
    }

    /**
     * Returns {@code true} if the described class has {@code readResolve()} method.
     *
     * @return {@code true} if the described class has {@code readResolve()} method
     */
    public boolean hasReadResolve() {
        return serialization.hasReadResolve();
    }

    /**
     * Returns {@code true} if this is the descriptor of {@code null} values.
     *
     * @return {@code true} if this is the descriptor of {@code null} values
     */
    public boolean isNull() {
        return descriptorId == BuiltInType.NULL.descriptorId();
    }

    /**
     * Returns {@code true} if this is the descriptor of {@link java.util.Collections#singletonList(Object)} type.
     *
     * @return {@code true} if this is the descriptor of {@link java.util.Collections#singletonList(Object)} type
     */
    public boolean isSingletonList() {
        return descriptorId == BuiltInType.SINGLETON_LIST.descriptorId();
    }

    /**
     * Returns {@code true} if the described class has writeReplace() method, and it makes sense for the needs of
     * our serialization (i.e. it is SERIALIZABLE or EXTERNALIZABLE).
     *
     * @return {@code true} if the described class has writeReplace() method, and it makes sense for the needs of
     *     our serialization
     */
    public boolean supportsWriteReplace() {
        return (isSerializable() || isExternalizable()) && hasWriteReplace();
    }

    /**
     * Returns {@code true} if the described class is a proxy.
     *
     * @return {@code true} if the described class is a proxy
     */
    public boolean isProxy() {
        return descriptorId == BuiltInType.PROXY.descriptorId();
    }

    /**
     * Returns special serialization methods facility.
     *
     * @return special serialization methods facility
     */
    public SpecialSerializationMethods serializationMethods() {
        return serializationMethods;
    }

    /**
     * Returns {@code true} if this descriptor describes same class as the given descriptor.
     *
     * @param other a descriptor to match against
     * @return {@code true} if this descriptor describes same class as the given descriptor
     */
    public boolean describesSameClass(ClassDescriptor other) {
        return other.clazz() == clazz();
    }

    /**
     * Returns total number of bytes needed to store all primitive fields.
     *
     * @return total number of bytes needed to store all primitive fields
     */
    public int primitiveFieldsDataSize() {
        return primitiveFieldsDataSize;
    }

    /**
     * Returns total number of object (i.e. non-primitive) fields.
     *
     * @return total number of object (i.e. non-primitive) fields
     */
    public int objectFieldsCount() {
        return objectFieldsCount;
    }

    /**
     * Returns size of the nulls bitmap for the described class; it is equal to the number of nullable (i.e. non-primitive)
     * fields that have a type known upfront.
     *
     * @return size of the nulls bitmap for the described class
     * @see #fieldIndexInNullsBitmap(String)
     * @see #isRuntimeTypeKnownUpfront()
     */
    public int fieldIndexInNullsBitmapSize() {
        return fieldNullsBitmapSize;
    }

    /**
     * Returns index of a field in the nulls bitmap for the described class (if it's nullable and its type is known upfront),
     * or -1 otherwise.
     *
     * @param fieldName name of the field
     * @return index of a field in the nulls bitmap for the described class (if it's nullable and its type is known upfront),
     *     or -1 otherwise
     */
    public int fieldIndexInNullsBitmap(String fieldName) {
        if (!fieldNullsBitmapIndices.containsKey(fieldName)) {
            throw new IllegalStateException("Unknown field " + fieldName);
        }

        return fieldNullsBitmapIndices.getInt(fieldName);
    }

    /**
     * Return offset into primitive fields data (which has size {@link #primitiveFieldsDataSize()}).
     * These are different from the offsets used in the context of {@link sun.misc.Unsafe}.
     *
     * @param fieldName    primitive field name
     * @param requiredType field type
     * @return offset into primitive fields data
     */
    public int primitiveFieldDataOffset(String fieldName, Class<?> requiredType) {
        assert requiredType.isPrimitive();

        FieldDescriptor fieldDesc = requiredFieldByName(fieldName);
        if (fieldDesc.clazz() != requiredType) {
            throw new IllegalStateException("Field " + fieldName + " has type " + fieldDesc.clazz()
                    + ", but it was used as " + requiredType);
        }

        if (primitiveFieldDataOffsets == null) {
            primitiveFieldDataOffsets = primitiveFieldDataOffsetsMap(fields);
        }

        assert primitiveFieldDataOffsets.containsKey(fieldName);

        return primitiveFieldDataOffsets.getInt(fieldName);
    }

    private FieldDescriptor requiredFieldByName(String fieldName) {
        if (fieldsByName == null) {
            fieldsByName = fieldsByNameMap(fields);
        }

        FieldDescriptor fieldDesc = fieldsByName.get(fieldName);
        if (fieldDesc == null) {
            throw new IllegalStateException("Did not find a field with name " + fieldName);
        }

        return fieldDesc;
    }

    private static Map<String, FieldDescriptor> fieldsByNameMap(List<FieldDescriptor> fields) {
        return fields.stream()
                .collect(toUnmodifiableMap(FieldDescriptor::name, Function.identity()));
    }

    private static Object2IntMap<String> primitiveFieldDataOffsetsMap(List<FieldDescriptor> fields) {
        Object2IntMap<String> map = new Object2IntOpenHashMap<>();

        int accumulatedOffset = 0;
        for (FieldDescriptor fieldDesc : fields) {
            if (fieldDesc.isPrimitive()) {
                map.put(fieldDesc.name(), accumulatedOffset);
                accumulatedOffset += Primitives.widthInBytes(fieldDesc.clazz());
            }
        }

        return Object2IntMaps.unmodifiable(map);
    }

    /**
     * Returns index of a non-primitive (i.e. object) field in the object fields array.
     *
     * @param fieldName object field name
     * @return index of a non-primitive (i.e. object) field in the object fields array
     */
    public int objectFieldIndex(String fieldName) {
        if (objectFieldIndices == null) {
            objectFieldIndices = computeObjectFieldIndices(fields);
        }

        if (!objectFieldIndices.containsKey(fieldName)) {
            throw new IllegalStateException("Did not find an object field with name " + fieldName);
        }

        return objectFieldIndices.getInt(fieldName);
    }

    private Object2IntMap<String> computeObjectFieldIndices(List<FieldDescriptor> fields) {
        Object2IntMap<String> map = new Object2IntOpenHashMap<>();

        int currentIndex = 0;
        for (FieldDescriptor fieldDesc : fields) {
            if (!fieldDesc.isPrimitive()) {
                map.put(fieldDesc.name(), currentIndex);
                currentIndex++;
            }
        }

        return Object2IntMaps.unmodifiable(map);
    }

    /**
     * Returns {@code true} if the descriptor describes an enum class.
     *
     * @return {@code true} if the descriptor describes an enum class
     */
    public boolean isEnum() {
        return Classes.isRuntimeEnum(clazz);
    }

    /**
     * Returns {@code true} if a field (or array item) of the described class can only host (at runtime) instances of this type
     * (and not subtypes), so the runtime type is known upfront. This is also true for enums, even though technically their values
     * might have subtypes; but we serialize them using their names, so we still treat the type as known upfront.
     *
     * @return {@code true} if a field (or array item) of the described class can only host (at runtime) instances of the concrete type
     *     that is known upfront
     */
    public boolean isRuntimeTypeKnownUpfront() {
        return Classes.isRuntimeTypeKnownUpfront(clazz);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return "ClassDescriptor{"
                + "className='" + className() + '\''
                + ", descriptorId=" + descriptorId
                + '}';
    }
}
