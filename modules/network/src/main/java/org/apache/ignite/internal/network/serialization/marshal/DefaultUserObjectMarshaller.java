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

package org.apache.ignite.internal.network.serialization.marshal;

import static org.apache.ignite.internal.network.serialization.marshal.ObjectClass.objectClass;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.network.serialization.BuiltinType;
import org.apache.ignite.internal.network.serialization.ClassDescriptor;
import org.apache.ignite.internal.network.serialization.ClassDescriptorFactory;
import org.apache.ignite.internal.network.serialization.ClassDescriptorFactoryContext;
import org.apache.ignite.internal.network.serialization.Null;
import org.jetbrains.annotations.Nullable;

/**
 * Default implementation of {@link UserObjectMarshaller}.
 */
public class DefaultUserObjectMarshaller implements UserObjectMarshaller {
    private final ClassDescriptorFactoryContext descriptorRegistry;
    private final ClassDescriptorFactory descriptorFactory;

    private final SpecialSerializationMethodsCache serializationMethodsCache = new SpecialSerializationMethodsCache();

    private final BuiltInNonContainerMarshallers builtInNonContainerMarshallers = new BuiltInNonContainerMarshallers();
    private final BuiltInContainerMarshallers builtInContainerMarshallers = new BuiltInContainerMarshallers(this::marshalToOutput);

    public DefaultUserObjectMarshaller(ClassDescriptorFactoryContext descriptorRegistry, ClassDescriptorFactory descriptorFactory) {
        this.descriptorRegistry = descriptorRegistry;
        this.descriptorFactory = descriptorFactory;
    }

    public MarshalledObject marshal(@Nullable Object object) throws MarshalException {
        return marshal(object, objectClass(object));
    }

    /** {@inheritDoc} */
    @Override
    public MarshalledObject marshal(@Nullable Object object, Class<?> declaredClass) throws MarshalException {
        Set<ClassDescriptor> usedDescriptors;

        var baos = new ByteArrayOutputStream();
        try (var dos = new DataOutputStream(baos)) {
            usedDescriptors = marshalToOutput(object, declaredClass, dos);
        } catch (IOException e) {
            throw new MarshalException("Cannot marshal", e);
        }

        return new MarshalledObject(baos.toByteArray(), usedDescriptors);
    }

    private Set<ClassDescriptor> marshalToOutput(Object element, DataOutput output) throws MarshalException, IOException {
        return marshalToOutput(element, objectClass(element), output);
    }

    private Set<ClassDescriptor> marshalToOutput(@Nullable Object object, Class<?> declaredClass, DataOutput output)
            throws MarshalException, IOException {
        assert declaredClass != null;
        assert object == null
                || declaredClass.isPrimitive()
                || objectIsMemberOfEnumWithAnonymousClassesForMembers(object, declaredClass)
                || object.getClass() == declaredClass
                : "Object " + object + " is expected to have class " + declaredClass + ", but its " + object.getClass();

        DescribedObject writeReplaced = applyWriteReplaceIfNeeded(object, declaredClass);

        writeDescriptorId(writeReplaced.descriptor, output);

        return writeObject(writeReplaced.object, writeReplaced.descriptor, output);
    }

    private boolean objectIsMemberOfEnumWithAnonymousClassesForMembers(Object object, Class<?> declaredClass) {
        return declaredClass.isEnum() && object.getClass().getSuperclass() == declaredClass;
    }

    private DescribedObject applyWriteReplaceIfNeeded(@Nullable Object originalObject, Class<?> declaredClass) throws MarshalException {
        final ClassDescriptor originalDescriptor = getOrCreateDescriptor(declaredClass);

        if (!originalDescriptor.supportsWriteReplace()) {
            return new DescribedObject(originalObject, originalDescriptor);
        }

        @Nullable Object objectToWrite = applyWriteReplace(originalObject, originalDescriptor);
        ClassDescriptor descriptorToUse = getOrCreateDescriptor(objectToWrite, objectClass(objectToWrite));

        return new DescribedObject(objectToWrite, descriptorToUse);
    }

    private Object applyWriteReplace(Object object, ClassDescriptor descriptor) throws MarshalException {
        return serializationMethodsCache.methodsFor(descriptor).writeReplace(object);
    }

    private ClassDescriptor getOrCreateDescriptor(@Nullable Object object, Class<?> declaredClass) {
        assert object != null || declaredClass == Void.class || declaredClass == Null.class;

        if (declaredClass == Void.class) {
            return descriptorRegistry.getDescriptor(Void.class);
        }

        if (object == null) {
            return descriptorRegistry.getNullDescriptor();
        }

        return getOrCreateDescriptor(object.getClass());
    }

    private ClassDescriptor getOrCreateDescriptor(Class<?> objectClass) {
        // ENUM and ENUM_ARRAY need to be handled separately because an enum value usually has a class different from
        // Enum and an ENUM_ARRAY might be used for both Enum[] and EnumSubclass[].
        if (objectClass.isEnum()) {
            return descriptorRegistry.getEnumDescriptor();
        }
        if (isEnumArray(objectClass)) {
            return descriptorRegistry.getDescriptor(Enum[].class);
        }

        ClassDescriptor descriptor = descriptorRegistry.getDescriptor(objectClass);
        if (descriptor != null) {
            return descriptor;
        }

        // This is some custom class (not a built-in). If it's a non-built-in array, we need handle it as a generic container.
        if (objectClass.isArray()) {
            return descriptorRegistry.getBuiltInDescriptor(BuiltinType.OBJECT_ARRAY);
        }

        descriptor = descriptorFactory.create(objectClass);
        return descriptor;
    }

    private boolean isEnumArray(Class<?> objectClass) {
        return objectClass.isArray() && objectClass.getComponentType().isEnum();
    }

    private void writeDescriptorId(ClassDescriptor descriptor, DataOutput output) throws IOException {
        output.writeInt(descriptor.descriptorId());
    }

    private Set<ClassDescriptor> writeObject(@Nullable Object object, ClassDescriptor descriptor, DataOutput output)
            throws IOException, MarshalException {
        if (descriptor.isNull()) {
            return Set.of(descriptor);
        } else if (isBuiltInNonContainer(descriptor)) {
            return builtInNonContainerMarshallers.writeBuiltIn(object, descriptor, output);
        } else if (isBuiltInCollection(descriptor)) {
            return builtInContainerMarshallers.writeBuiltInCollection((Collection<?>) object, descriptor, output);
        } else if (isBuiltInMap(descriptor)) {
            return builtInContainerMarshallers.writeBuiltInMap((Map<?, ?>) object, descriptor, output);
        } else if (isArray(descriptor)) {
            return builtInContainerMarshallers.writeGenericRefArray((Object[]) object, descriptor, output);
        } else if (descriptor.isExternalizable()) {
            return writeExternalizable((Externalizable) object, descriptor, output);
        } else {
            throw new UnsupportedOperationException("Not supported yet");
        }
    }

    private boolean isBuiltInNonContainer(ClassDescriptor descriptor) {
        return descriptor.isBuiltIn() && builtInNonContainerMarshallers.supports(descriptor.clazz());
    }

    private boolean isArray(ClassDescriptor descriptor) {
        return descriptor.clazz().isArray();
    }

    private boolean isBuiltInCollection(ClassDescriptor descriptor) {
        return descriptor.isBuiltIn() && Collection.class.isAssignableFrom(descriptor.clazz());
    }

    private boolean isBuiltInMap(ClassDescriptor descriptor) {
        return descriptor.isBuiltIn() && Map.class.isAssignableFrom(descriptor.clazz());
    }

    private Set<ClassDescriptor> writeExternalizable(Externalizable externalizable, ClassDescriptor descriptor, DataOutput output)
            throws IOException {
        byte[] externalizableBytes = externalize(externalizable);

        output.writeInt(externalizableBytes.length);
        output.write(externalizableBytes);

        return Set.of(descriptor);
    }

    private byte[] externalize(Externalizable externalizable) throws IOException {
        var baos = new ByteArrayOutputStream();
        try (var oos = new ObjectOutputStream(baos)) {
            externalizable.writeExternal(oos);
        }

        return baos.toByteArray();
    }

    /** {@inheritDoc} */
    @Override
    @Nullable
    public <T> T unmarshal(byte[] bytes) throws UnmarshalException {
        try (var dis = new DataInputStream(new ByteArrayInputStream(bytes))) {
            return unmarshalFromInput(dis);
        } catch (IOException e) {
            throw new UnmarshalException("Cannot unmarshal", e);
        }
    }

    private <T> T unmarshalFromInput(DataInput input) throws IOException, UnmarshalException {
        int descriptorId = readDescriptorId(input);
        ClassDescriptor descriptor = descriptorRegistry.getRequiredDescriptor(descriptorId);

        Object readObject = readObject(input, descriptor);
        @SuppressWarnings("unchecked") T resolvedObject = (T) readResolveIfNeeded(readObject, descriptor);
        return resolvedObject;
    }

    private int readDescriptorId(DataInput input) throws IOException {
        return input.readInt();
    }

    @Nullable
    private Object readObject(DataInput input, ClassDescriptor descriptor) throws IOException, UnmarshalException {
        if (descriptor.isNull()) {
            return null;
        } else if (isBuiltInNonContainer(descriptor)) {
            return builtInNonContainerMarshallers.readBuiltIn(descriptor, input);
        } else if (isBuiltInCollection(descriptor)) {
            return readBuiltInCollection(input, descriptor);
        } else if (isBuiltInMap(descriptor)) {
            return readBuiltInMap(input, descriptor);
        } else if (isArray(descriptor)) {
            return readGenericRefArray(input);
        } else if (descriptor.isExternalizable()) {
            return readExternalizable(descriptor, input);
        } else {
            throw new UnsupportedOperationException("Not supported yet");
        }
    }

    private Object[] readGenericRefArray(DataInput input) throws IOException, UnmarshalException {
        return builtInContainerMarshallers.readGenericRefArray(input, this::unmarshalFromInput);
    }

    private Collection<Object> readBuiltInCollection(DataInput input, ClassDescriptor descriptor) throws UnmarshalException, IOException {
        return builtInContainerMarshallers.readBuiltInCollection(descriptor, this::unmarshalFromInput, input);
    }

    private Map<Object, Object> readBuiltInMap(DataInput input, ClassDescriptor descriptor) throws UnmarshalException, IOException {
        return builtInContainerMarshallers.readBuiltInMap(descriptor, this::unmarshalFromInput, this::unmarshalFromInput, input);
    }

    private <T extends Externalizable> T readExternalizable(ClassDescriptor descriptor, DataInput input)
            throws IOException, UnmarshalException {
        T object = instantiateObject(descriptor);

        int length = input.readInt();
        byte[] bytes = new byte[length];
        input.readFully(bytes);

        try (var ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            object.readExternal(ois);
        } catch (ClassNotFoundException e) {
            throw new UnmarshalException("Cannot unmarshal due to a missing class", e);
        }

        return object;
    }

    @SuppressWarnings("unchecked")
    private <T extends Externalizable> T instantiateObject(ClassDescriptor descriptor) throws UnmarshalException {
        try {
            return (T) descriptor.clazz().getConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new UnmarshalException("Cannot instantiate " + descriptor.clazz(), e);
        }
    }

    private Object readResolveIfNeeded(Object readObject, ClassDescriptor descriptor) throws UnmarshalException {
        if (descriptor.hasReadResolve()) {
            return applyReadResolve(readObject, descriptor);
        } else {
            return readObject;
        }
    }

    private Object applyReadResolve(Object object, ClassDescriptor descriptor) throws UnmarshalException {
        return serializationMethodsCache.methodsFor(descriptor).readResolve(object);
    }

    private static class DescribedObject {
        @Nullable
        private final Object object;
        private final ClassDescriptor descriptor;

        private DescribedObject(@Nullable Object object, ClassDescriptor descriptor) {
            this.object = object;
            this.descriptor = descriptor;
        }
    }
}
