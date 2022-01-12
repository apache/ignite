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

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

import java.io.Externalizable;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;

/**
 * Class descriptor factory for the user object serialization.
 */
public class ClassDescriptorFactory {
    /** Means that no serialization override is present; used for readability instead of {@code false}. */
    private static final boolean NO_SERIALIZATION_OVERRIDE = false;
    /** Means that no readObjectNoData() method is present; used for readability instead of {@code false}. */
    private static final boolean NO_READ_OBJECT_NO_DATA = false;

    /**
     * Factory context.
     */
    private final ClassDescriptorFactoryContext context;

    /**
     * Constructor.
     *
     * @param ctx Context.
     */
    public ClassDescriptorFactory(ClassDescriptorFactoryContext ctx) {
        this.context = ctx;
    }

    /**
     * Creates the class' descriptor and descriptors of class' fields if they're not already created.
     *
     * @param clazz Class definition.
     * @return Class descriptor.
     */
    public ClassDescriptor create(Class<?> clazz) {
        ClassDescriptor classDesc = create0(clazz);

        context.addDescriptor(classDesc);

        Queue<FieldDescriptor> fieldDescriptors = new ArrayDeque<>(classDesc.fields());

        while (!fieldDescriptors.isEmpty()) {
            FieldDescriptor fieldDescriptor = fieldDescriptors.remove();

            int typeDescriptorId = fieldDescriptor.typeDescriptorId();

            if (context.hasDescriptor(typeDescriptorId)) {
                continue;
            }

            Class<?> fieldClass = fieldDescriptor.clazz();

            ClassDescriptor fieldClassDesc = create0(fieldClass);

            context.addDescriptor(fieldClassDesc);

            fieldDescriptors.addAll(fieldClassDesc.fields());
        }

        return classDesc;
    }

    /**
     * Creates the class' descriptor.
     *
     * @param clazz Class.
     * @return Class' descriptor.
     */
    private ClassDescriptor create0(Class<?> clazz) {
        assert !clazz.isPrimitive() :
            clazz + " is a primitive, there should be a default descriptor";

        int descriptorId = context.getId(clazz);

        if (Externalizable.class.isAssignableFrom(clazz)) {
            //noinspection unchecked
            return externalizable(descriptorId, (Class<? extends Externalizable>) clazz);
        } else if (Serializable.class.isAssignableFrom(clazz)) {
            //noinspection unchecked
            return serializable(descriptorId, (Class<? extends Serializable>) clazz);
        } else {
            return arbitrary(descriptorId, clazz);
        }
    }

    /**
     * Parses the externalizable class definition.
     *
     * @param descriptorId Descriptor id of the class.
     * @param clazz        Externalizable class.
     * @return Class descriptor.
     */
    private ClassDescriptor externalizable(int descriptorId, Class<? extends Externalizable> clazz) {
        checkHasPublicNoArgConstructor(clazz);

        return new ClassDescriptor(
                clazz,
                descriptorId,
                superClassDescriptor(clazz),
                Collections.emptyList(),
                new Serialization(
                        SerializationType.EXTERNALIZABLE,
                        NO_SERIALIZATION_OVERRIDE,
                        NO_READ_OBJECT_NO_DATA,
                        hasWriteReplace(clazz),
                        hasReadResolve(clazz)
                )
        );
    }

    /**
     * If the given class has a super-class (which is not Object) and the class is not an Enum subclass, parses the super-class
     * and registers the resulting descriptor.
     *
     * @param clazz class which super-class to parse
     * @return descriptor of the super-class or {@code null} if the class is an enum, or it has no super-class, or the super-class is Object
     */
    private ClassDescriptor superClassDescriptor(Class<?> clazz) {
        if (Enum.class.isAssignableFrom(clazz)) {
            return null;
        }

        Class<?> superclass = clazz.getSuperclass();

        if (superclass == null || superclass == Object.class) {
            return null;
        }

        return create(superclass);
    }

    /**
     * Checks if a class has a public no-arg constructor.
     *
     * @param clazz Class.
     */
    private static void checkHasPublicNoArgConstructor(Class<? extends Externalizable> clazz) throws IgniteException {
        boolean hasPublicNoArgConstructor = true;

        try {
            Constructor<? extends Externalizable> ctor = clazz.getConstructor();

            if (!Modifier.isPublic(ctor.getModifiers())) {
                hasPublicNoArgConstructor = false;
            }
        } catch (NoSuchMethodException e) {
            hasPublicNoArgConstructor = false;
        }

        if (!hasPublicNoArgConstructor) {
            throw new IgniteException(
                "Externalizable class " + clazz.getName() + " has no public no-arg constructor");
        }
    }

    /**
     * Parses the serializable class definition.
     *
     * @param descriptorId Descriptor id of the class.
     * @param clazz Serializable class.
     * @return Class descriptor.
     */
    private ClassDescriptor serializable(int descriptorId, Class<? extends Serializable> clazz) {
        return new ClassDescriptor(
                clazz,
                descriptorId,
                superClassDescriptor(clazz),
                fields(clazz),
                new Serialization(
                        SerializationType.SERIALIZABLE,
                        hasOverrideSerialization(clazz),
                        hasReadObjectNoData(clazz),
                        hasWriteReplace(clazz),
                        hasReadResolve(clazz)
                )
        );
    }

    private boolean hasReadResolve(Class<? extends Serializable> clazz) {
        return getReadResolve(clazz) != null;
    }

    private boolean hasWriteReplace(Class<? extends Serializable> clazz) {
        return getWriteReplace(clazz) != null;
    }

    private boolean hasOverrideSerialization(Class<? extends Serializable> clazz) {
        return hasWriteObject(clazz) && hasReadObject(clazz);
    }

    private boolean hasReadObject(Class<? extends Serializable> clazz) {
        return getReadObject(clazz) != null;
    }

    private boolean hasWriteObject(Class<? extends Serializable> clazz) {
        return getWriteObject(clazz) != null;
    }

    private boolean hasReadObjectNoData(Class<? extends Serializable> clazz) {
        return getReadObjectNoData(clazz) != null;
    }

    /**
     * Parses the arbitrary class (not serializable or externalizable) definition.
     *
     * @param descriptorId Descriptor id of the class.
     * @param clazz Arbitrary class.
     * @return Class descriptor.
     */
    private ClassDescriptor arbitrary(int descriptorId, Class<?> clazz) {
        return new ClassDescriptor(
                clazz,
                descriptorId,
                superClassDescriptor(clazz),
                fields(clazz),
                new Serialization(SerializationType.ARBITRARY)
        );
    }

    /**
     * Returns descriptors of 'serializable' (i.e. non-static non-transient) declared fields of the given class
     * sorted lexicographically by their names.
     *
     * @param clazz class
     * @return properly sorted field descriptors
     */
    private List<FieldDescriptor> fields(Class<?> clazz) {
        return Arrays.stream(clazz.getDeclaredFields())
                .sorted(comparing(Field::getName))
                .filter(field -> {
                    int modifiers = field.getModifiers();

                    // Ignore static and transient fields.
                    return !Modifier.isStatic(modifiers) && !Modifier.isTransient(modifiers);
                })
                .map(field -> new FieldDescriptor(field, context.getId(field.getType())))
                .collect(toList());
    }

    /**
     * Gets a method with the signature
     * {@code ANY-ACCESS-MODIFIER Object writeReplace() throws ObjectStreamException}.
     *
     * @param clazz Class.
     * @return Method.
     */
    @Nullable
    private static Method getWriteReplace(Class<? extends Serializable> clazz) {
        try {
            return clazz.getDeclaredMethod("writeReplace");
        } catch (NoSuchMethodException e) {
            return null;
        }
    }

    /**
     * Gets a method with the signature
     * {@code ANY-ACCESS-MODIFIER Object readResolve() throws ObjectStreamException}.
     *
     * @param clazz Class.
     * @return Method.
     */
    @Nullable
    private static Method getReadResolve(Class<? extends Serializable> clazz) {
        try {
            return clazz.getDeclaredMethod("readResolve");
        } catch (NoSuchMethodException e) {
            return null;
        }
    }

    /**
     * Gets a method with the signature
     * {@code private void writeObject(java.io.ObjectOutputStream out) throws IOException}.
     *
     * @param clazz Class.
     * @return Method.
     */
    @Nullable
    private static Method getWriteObject(Class<? extends Serializable> clazz) {
        try {
            Method writeObject = clazz.getDeclaredMethod("writeObject", ObjectOutputStream.class);

            if (!Modifier.isPrivate(writeObject.getModifiers())) {
                return null;
            }

            return writeObject;
        } catch (NoSuchMethodException e) {
            return null;
        }
    }

    /**
     * Gets a method with the signature
     * {@code private void readObject(java.io.ObjectInputStream in) throws IOException,
     * ClassNotFoundException}.
     *
     * @param clazz Class.
     * @return Method.
     */
    @Nullable
    private static Method getReadObject(Class<? extends Serializable> clazz) {
        try {
            Method writeObject = clazz.getDeclaredMethod("readObject", ObjectInputStream.class);

            if (!Modifier.isPrivate(writeObject.getModifiers())) {
                return null;
            }

            return writeObject;
        } catch (NoSuchMethodException e) {
            return null;
        }
    }

    /**
     * Gets a method with the signature
     * {@code private void readObjectNoData() throws ObjectStreamException}.
     *
     * @param clazz Class.
     * @return Method.
     */
    @Nullable
    private static Method getReadObjectNoData(Class<? extends Serializable> clazz) {
        try {
            Method writeObject = clazz.getDeclaredMethod("readObjectNoData");

            if (!Modifier.isPrivate(writeObject.getModifiers())) {
                return null;
            }

            return writeObject;
        } catch (NoSuchMethodException e) {
            return null;
        }
    }
}
