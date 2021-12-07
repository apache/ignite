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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;

/**
 * Class descriptor factory for the user object serialization.
 */
public class ClassDescriptorFactory {
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
            Collections.emptyList(),
            SerializationType.EXTERNALIZABLE
        );
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
        Method writeObject = getWriteObject(clazz);
        Method readObject = getReadObject(clazz);
        Method readObjectNoData = getReadObjectNoData(clazz);

        boolean overrideSerialization = writeObject != null && readObject != null && readObjectNoData != null;

        Method writeReplace = getWriteReplace(clazz);
        Method readResolve = getReadResolve(clazz);

        int serializationType = SerializationType.SERIALIZABLE;

        if (overrideSerialization) {
            serializationType |= SerializationType.SERIALIZABLE_OVERRIDE;
        }

        if (writeReplace != null) {
            serializationType |= SerializationType.SERIALIZABLE_WRITE_REPLACE;
        }

        if (readResolve != null) {
            serializationType |= SerializationType.SERIALIZABLE_READ_RESOLVE;
        }

        return new ClassDescriptor(clazz, descriptorId, fields(clazz), serializationType);
    }

    /**
     * Parses the arbitrary class (not serializable or externalizable) definition.
     *
     * @param descriptorId Descriptor id of the class.
     * @param clazz Arbitrary class.
     * @return Class descriptor.
     */
    private ClassDescriptor arbitrary(int descriptorId, Class<?> clazz) {
        return new ClassDescriptor(clazz, descriptorId, fields(clazz), SerializationType.ARBITRARY);
    }

    /**
     * Gets field descriptors of the class. If a field's type doesn't have an id yet, generates it.
     *
     * @param clazz Class.
     * @return List of field descriptor.
     */
    private List<FieldDescriptor> fields(Class<?> clazz) {
        if (clazz.getSuperclass() != Object.class) {
            // TODO: IGNITE-15945 add support for the inheritance
            throw new UnsupportedOperationException("IGNITE-15945");
        }

        return Arrays.stream(clazz.getDeclaredFields())
            .filter(field -> {
                int modifiers = field.getModifiers();

                // Ignore static and transient field.
                return !Modifier.isStatic(modifiers) && !Modifier.isTransient(modifiers);
            })
            .map(field -> new FieldDescriptor(field, context.getId(field.getType())))
            .collect(Collectors.toList());
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
            Method writeReplace = clazz.getDeclaredMethod("writeReplace");

            if (!declaresExactExceptions(writeReplace, Set.of(ObjectStreamException.class))) {
                return null;
            }

            return writeReplace;
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
            Method readResolve = clazz.getDeclaredMethod("readResolve");

            if (!declaresExactExceptions(readResolve, Set.of(ObjectStreamException.class))) {
                return null;
            }

            return readResolve;
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

            if (!declaresExactExceptions(writeObject, Set.of(IOException.class))) {
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

            if (!declaresExactExceptions(writeObject, Set.of(IOException.class, ClassNotFoundException.class))) {
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

            if (!declaresExactExceptions(writeObject, Set.of(ObjectStreamException.class))) {
                return null;
            }

            return writeObject;
        } catch (NoSuchMethodException e) {
            return null;
        }
    }

    /**
     * Returns {@code true} if the method's declaration contains throwing only of
     * specified exceptions.
     *
     * @param method Method.
     * @param exceptions Set of exceptions.
     * @return If the method throws exceptions.
     */
    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private static boolean declaresExactExceptions(Method method, Set<Class<? extends Throwable>> exceptions) {
        Class<?>[] exceptionTypes = method.getExceptionTypes();

        if (exceptionTypes.length != exceptions.size()) {
            return false;
        }

        return Arrays.asList(exceptionTypes).containsAll(exceptions);
    }
}
