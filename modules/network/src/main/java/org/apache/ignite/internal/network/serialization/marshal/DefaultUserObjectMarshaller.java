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
import java.io.InputStream;
import java.io.InvalidObjectException;
import java.util.Collection;
import java.util.Map;
import org.apache.ignite.internal.network.serialization.BuiltInType;
import org.apache.ignite.internal.network.serialization.ClassDescriptor;
import org.apache.ignite.internal.network.serialization.ClassDescriptorFactory;
import org.apache.ignite.internal.network.serialization.ClassDescriptorRegistry;
import org.apache.ignite.internal.network.serialization.DescriptorRegistry;
import org.apache.ignite.internal.network.serialization.SpecialMethodInvocationException;
import org.jetbrains.annotations.Nullable;

/**
 * Default implementation of {@link UserObjectMarshaller}.
 */
public class DefaultUserObjectMarshaller implements UserObjectMarshaller {
    private static final boolean UNSHARED = true;
    private static final boolean NOT_UNSHARED = false;

    private static final Class<?> NO_DECLARED_CLASS = null;

    private final LocalDescriptors localDescriptors;

    private final BuiltInNonContainerMarshallers builtInNonContainerMarshallers = new BuiltInNonContainerMarshallers();
    private final BuiltInContainerMarshallers builtInContainerMarshallers = new BuiltInContainerMarshallers(
            this::marshalShared,
            this::unmarshalShared
    );
    private final StructuredObjectMarshaller structuredObjectMarshaller;
    private final ExternalizableMarshaller externalizableMarshaller;
    private final ProxyMarshaller proxyMarshaller;

    private final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

    /**
     * Constructor.
     *
     * @param localRegistry registry of local descriptors to consult with (and to write to if an unseen class is encountered)
     * @param descriptorFactory  descriptor factory to create new descriptors from classes
     */
    public DefaultUserObjectMarshaller(ClassDescriptorRegistry localRegistry, ClassDescriptorFactory descriptorFactory) {
        localDescriptors = new LocalDescriptors(localRegistry, descriptorFactory);

        structuredObjectMarshaller = new StructuredObjectMarshaller(
                localRegistry,
                this::marshalShared,
                this::marshalUnshared,
                this::unmarshalShared,
                this::unmarshalUnshared
        );

        externalizableMarshaller = new ExternalizableMarshaller(
                this::marshalShared,
                this::marshalUnshared,
                this::unmarshalShared,
                this::unmarshalUnshared,
                structuredObjectMarshaller
        );

        proxyMarshaller = new ProxyMarshaller(this::marshalShared, this::unmarshalShared);
    }

    /** {@inheritDoc} */
    @Override
    public MarshalledObject marshal(@Nullable Object object) throws MarshalException {
        MarshallingContext context = new MarshallingContext();

        var baos = new ByteArrayOutputStream();
        try (var dos = new DataOutputStream(baos)) {
            marshalShared(object, dos, context);
        } catch (IOException e) {
            throw new MarshalException("Cannot marshal", e);
        }

        return new MarshalledObject(baos.toByteArray(), context.usedDescriptors());
    }

    private void marshalShared(@Nullable Object object, DataOutputStream output, MarshallingContext context)
            throws MarshalException, IOException {
        marshalShared(object, NO_DECLARED_CLASS, output, context);
    }

    private void marshalShared(
            @Nullable Object object,
            @Nullable Class<?> declaredClass,
            DataOutputStream output,
            MarshallingContext context
    ) throws MarshalException, IOException {
        marshalToOutput(object, declaredClass, output, context, NOT_UNSHARED);
    }

    private void marshalUnshared(@Nullable Object object, Class<?> declaredClass, DataOutputStream output, MarshallingContext context)
            throws MarshalException, IOException {
        marshalToOutput(object, declaredClass, output, context, UNSHARED);
    }

    private void marshalToOutput(
            @Nullable Object object,
            @Nullable Class<?> declaredClass,
            DataOutputStream output,
            MarshallingContext context,
            boolean unshared
    ) throws MarshalException, IOException {
        assert object == null
                || declaredClass == null
                || declaredClass.isPrimitive()
                || objectIsMemberOfEnumWithAnonymousClassesForMembers(object, declaredClass)
                || declaredClass.isAssignableFrom(object.getClass())
                : "Object " + object + " is expected to be an instance of subclass of " + declaredClass + ", but it's " + object.getClass();

        MarshallingValidations.throwIfMarshallingNotSupported(object);

        ClassDescriptor originalDescriptor = localDescriptors.getOrCreateDescriptor(object, declaredClass);

        DescribedObject afterReplacement = applyWriteReplaceIfNeeded(object, originalDescriptor);

        if (hasObjectIdentity(afterReplacement.object, afterReplacement.descriptor)) {
            long flaggedObjectId = context.memorizeObject(afterReplacement.object, unshared);
            int objectId = FlaggedObjectIds.objectId(flaggedObjectId);

            if (FlaggedObjectIds.isAlreadySeen(flaggedObjectId)) {
                writeReference(objectId, declaredClass, output);
            } else {
                marshalIdentifiable(afterReplacement.object, afterReplacement.descriptor, declaredClass, objectId, output, context);
            }
        } else {
            marshalValue(afterReplacement.object, afterReplacement.descriptor, declaredClass, output, context);
        }
    }

    private boolean objectIsMemberOfEnumWithAnonymousClassesForMembers(Object object, Class<?> declaredClass) {
        return declaredClass.isEnum() && object.getClass().getSuperclass() == declaredClass;
    }

    private DescribedObject applyWriteReplaceIfNeeded(@Nullable Object objectBefore, ClassDescriptor descriptorBefore)
            throws MarshalException {
        if (!descriptorBefore.supportsWriteReplace()) {
            return new DescribedObject(objectBefore, descriptorBefore);
        }

        Object replacedObject = applyWriteReplace(objectBefore, descriptorBefore);
        ClassDescriptor replacementDescriptor = localDescriptors.getOrCreateDescriptor(replacedObject, objectClass(replacedObject));

        if (descriptorBefore.describesSameClass(replacementDescriptor)) {
            return new DescribedObject(replacedObject, replacementDescriptor);
        } else {
            // Let's do it again!
            return applyWriteReplaceIfNeeded(replacedObject, replacementDescriptor);
        }
    }

    @Nullable
    private Object applyWriteReplace(Object originalObject, ClassDescriptor originalDescriptor) throws MarshalException {
        try {
            return originalDescriptor.serializationMethods().writeReplace(originalObject);
        } catch (SpecialMethodInvocationException e) {
            throw new MarshalException("Cannot apply writeReplace()", e);
        }
    }

    private boolean hasObjectIdentity(@Nullable Object object, ClassDescriptor descriptor) {
        return object != null && mayHaveObjectIdentity(descriptor);
    }

    private boolean mayHaveObjectIdentity(ClassDescriptor descriptor) {
        return !descriptor.clazz().isPrimitive() && !descriptor.isNull();
    }

    private void writeReference(int objectId, Class<?> declaredClass, DataOutput output) throws IOException {
        if (!runtimeTypeIsKnownUpfront(declaredClass)) {
            ProtocolMarshalling.writeDescriptorOrCommandId(BuiltInType.REFERENCE.descriptorId(), output);
        }
        ProtocolMarshalling.writeObjectId(objectId, output);
    }

    private void marshalIdentifiable(
            Object object,
            ClassDescriptor descriptor,
            @Nullable Class<?> declaredClass,
            int objectId,
            DataOutputStream output,
            MarshallingContext context
    ) throws IOException, MarshalException {
        if (!runtimeTypeIsKnownUpfront(declaredClass)) {
            writeDescriptorId(descriptor, output);
        }
        ProtocolMarshalling.writeObjectId(objectId, output);

        writeObject(object, descriptor, output, context);
    }

    private boolean runtimeTypeIsKnownUpfront(@Nullable Class<?> declaredClass) {
        if (declaredClass == null) {
            return false;
        }

        ClassDescriptor declaredClassDescriptor = localDescriptors.getOrCreateDescriptor(declaredClass);
        return declaredClassDescriptor.isRuntimeTypeKnownUpfront();
    }

    private void writeDescriptorId(ClassDescriptor descriptor, DataOutput output) throws IOException {
        ProtocolMarshalling.writeDescriptorOrCommandId(descriptor.descriptorId(), output);
    }

    private void marshalValue(
            Object object,
            ClassDescriptor descriptor,
            Class<?> declaredClass,
            DataOutputStream output,
            MarshallingContext context
    ) throws IOException, MarshalException {
        if (!runtimeTypeIsKnownUpfront(declaredClass)) {
            writeDescriptorId(descriptor, output);
        }

        writeObject(object, descriptor, output, context);
    }

    private void writeObject(@Nullable Object object, ClassDescriptor descriptor, DataOutputStream output, MarshallingContext context)
            throws IOException, MarshalException {
        if (isBuiltInNonContainer(descriptor)) {
            builtInNonContainerMarshallers.writeBuiltIn(object, descriptor, output, context);
        } else if (isBuiltInCollection(descriptor)) {
            builtInContainerMarshallers.writeBuiltInCollection((Collection<?>) object, descriptor, output, context);
        } else if (isBuiltInMap(descriptor)) {
            builtInContainerMarshallers.writeBuiltInMap((Map<?, ?>) object, descriptor, output, context);
        } else if (isArray(descriptor)) {
            //noinspection ConstantConditions
            builtInContainerMarshallers.writeGenericRefArray((Object[]) object, descriptor, output, context);
        } else if (descriptor.isExternalizable()) {
            externalizableMarshaller.writeExternalizable((Externalizable) object, descriptor, output, context);
        } else if (descriptor.isProxy()) {
            //noinspection ConstantConditions
            proxyMarshaller.writeProxy(object, output, context);
        } else {
            structuredObjectMarshaller.writeStructuredObject(object, descriptor, output, context);
        }
    }

    private boolean isBuiltInNonContainer(ClassDescriptor descriptor) {
        return builtInNonContainerMarshallers.supports(descriptor);
    }

    private boolean isBuiltInCollection(ClassDescriptor descriptor) {
        return descriptor.isBuiltIn() && Collection.class.isAssignableFrom(descriptor.clazz());
    }

    private boolean isBuiltInMap(ClassDescriptor descriptor) {
        return descriptor.isBuiltIn() && Map.class.isAssignableFrom(descriptor.clazz());
    }

    private boolean isArray(ClassDescriptor descriptor) {
        return descriptor.clazz().isArray();
    }

    /** {@inheritDoc} */
    @Override
    @Nullable
    public <T> T unmarshal(byte[] bytes, DescriptorRegistry mergedDescriptors) throws UnmarshalException {
        var bais = new ByteArrayInputStream(bytes);
        try (var dis = new DataInputStream(bais)) {
            UnmarshallingContext context = new UnmarshallingContext(bais, mergedDescriptors, classLoader);
            T result = unmarshalShared(dis, context);

            throwIfNotDrained(dis);

            return result;
        } catch (IOException e) {
            throw new UnmarshalException("Cannot unmarshal", e);
        }
    }

    private <T> T unmarshalShared(DataInputStream input, UnmarshallingContext context) throws IOException, UnmarshalException {
        return unmarshalShared(input, NO_DECLARED_CLASS, context);
    }

    private <T> T unmarshalShared(DataInputStream input, @Nullable Class<?> declaredClass, UnmarshallingContext context)
            throws IOException, UnmarshalException {
        return unmarshalFromInput(input, declaredClass, context, NOT_UNSHARED);
    }

    private <T> T unmarshalUnshared(DataInputStream input, @Nullable Class<?> declaredClass, UnmarshallingContext context)
            throws IOException, UnmarshalException {
        return unmarshalFromInput(input, declaredClass, context, UNSHARED);
    }

    private <T> T unmarshalFromInput(
            DataInputStream input,
            @Nullable Class<?> declaredClass,
            UnmarshallingContext context,
            boolean unshared
    ) throws IOException, UnmarshalException {
        ClassDescriptor descriptor = resolveDescriptor(input, declaredClass, context);

        if (mayHaveObjectIdentity(descriptor)) {
            int objectId = peekObjectId(input, context);
            if (context.isKnownObjectId(objectId)) {
                // this is a back-reference
                return unmarshalReference(input, context, unshared);
            }
        }

        Object readObject = readObject(input, context, descriptor, unshared);

        @SuppressWarnings("unchecked") T resolvedObject = (T) applyReadResolveIfNeeded(readObject, descriptor);
        return resolvedObject;
    }

    private ClassDescriptor resolveDescriptor(DataInputStream input, @Nullable Class<?> declaredClass, UnmarshallingContext context)
            throws UnmarshalException, IOException {
        if (runtimeTypeIsKnownUpfront(declaredClass)) {
            return context.resolveDescriptorOfDeclaredClass(declaredClass);
        } else {
            int commandOrDescriptorId = ProtocolMarshalling.readDescriptorOrCommandId(input);
            return context.getRequiredDescriptor(commandOrDescriptorId);
        }
    }

    private int peekObjectId(DataInputStream input, UnmarshallingContext context) throws IOException {
        context.markSource(ProtocolMarshalling.MAX_LENGTH_BYTE_COUNT);
        int objectId = ProtocolMarshalling.readObjectId(input);
        context.resetSourceToMark();
        return objectId;
    }

    private <T> T unmarshalReference(DataInput input, UnmarshallingContext context, boolean unshared) throws IOException {
        if (unshared) {
            throw new InvalidObjectException("cannot read back reference as unshared");
        }

        int objectId = ProtocolMarshalling.readObjectId(input);

        if (context.isUnsharedObjectId(objectId)) {
            throw new InvalidObjectException("cannot read back reference to unshared object");
        }

        return context.dereference(objectId);
    }

    @Nullable
    private Object readObject(DataInputStream input, UnmarshallingContext context, ClassDescriptor descriptor, boolean unshared)
            throws IOException, UnmarshalException {
        if (!mayHaveObjectIdentity(descriptor)) {
            return readValue(input, descriptor, context);
        } else if (mustBeReadInOneStage(descriptor)) {
            return readIdentifiableInOneStage(input, descriptor, context, unshared);
        } else {
            return readIdentifiableInTwoStages(input, descriptor, context, unshared);
        }
    }

    private boolean mustBeReadInOneStage(ClassDescriptor descriptor) {
        return builtInNonContainerMarshallers.supports(descriptor);
    }

    @Nullable
    private Object readIdentifiableInOneStage(
            DataInputStream input,
            ClassDescriptor descriptor,
            UnmarshallingContext context,
            boolean unshared
    ) throws IOException, UnmarshalException {
        int objectId = readObjectId(input);

        Object object = readValue(input, descriptor, context);
        context.registerReference(objectId, object, unshared);

        return object;
    }

    private int readObjectId(DataInputStream input) throws IOException {
        return ProtocolMarshalling.readObjectId(input);
    }

    private Object readIdentifiableInTwoStages(
            DataInputStream input,
            ClassDescriptor descriptor,
            UnmarshallingContext context,
            boolean unshared
    ) throws IOException, UnmarshalException {
        int objectId = readObjectId(input);

        Object preInstantiatedObject = preInstantiate(descriptor, input, context);
        context.registerReference(objectId, preInstantiatedObject, unshared);

        fillObjectFrom(input, preInstantiatedObject, descriptor, context);

        return preInstantiatedObject;
    }

    private Object preInstantiate(ClassDescriptor descriptor, DataInputStream input, UnmarshallingContext context)
            throws IOException, UnmarshalException {
        if (isBuiltInNonContainer(descriptor)) {
            throw new IllegalStateException("Should not be here, descriptor is " + descriptor);
        } else if (isBuiltInCollection(descriptor)) {
            return builtInContainerMarshallers.preInstantiateBuiltInMutableCollection(descriptor, input, context);
        } else if (isBuiltInMap(descriptor)) {
            return builtInContainerMarshallers.preInstantiateBuiltInMutableMap(descriptor, input, context);
        } else if (isArray(descriptor)) {
            return builtInContainerMarshallers.preInstantiateGenericRefArray(input, context);
        } else if (descriptor.isExternalizable()) {
            return externalizableMarshaller.preInstantiateExternalizable(descriptor);
        } else if (descriptor.isProxy()) {
            return proxyMarshaller.preInstantiateProxy(input, context);
        } else {
            return structuredObjectMarshaller.preInstantiateStructuredObject(descriptor);
        }
    }

    private void fillObjectFrom(DataInputStream input, Object objectToFill, ClassDescriptor descriptor, UnmarshallingContext context)
            throws UnmarshalException, IOException {
        if (isBuiltInNonContainer(descriptor)) {
            throw new IllegalStateException("Cannot fill " + descriptor.clazz() + ", this is a programmatic error");
        } else if (isBuiltInCollection(descriptor)) {
            fillBuiltInCollectionFrom(input, (Collection<?>) objectToFill, descriptor, context);
        } else if (isBuiltInMap(descriptor)) {
            fillBuiltInMapFrom(input, (Map<?, ?>) objectToFill, context);
        } else if (isArray(descriptor)) {
            fillGenericRefArrayFrom(input, (Object[]) objectToFill, context);
        } else if (descriptor.isExternalizable()) {
            externalizableMarshaller.fillExternalizableFrom(input, (Externalizable) objectToFill, context);
        } else if (descriptor.isProxy()) {
            proxyMarshaller.fillProxyFrom(input, objectToFill, context);
        } else {
            structuredObjectMarshaller.fillStructuredObjectFrom(input, objectToFill, descriptor, context);
        }
    }

    private void fillBuiltInCollectionFrom(
            DataInputStream input,
            Collection<?> collectionToFill,
            ClassDescriptor descriptor,
            UnmarshallingContext context
    ) throws UnmarshalException, IOException {
        builtInContainerMarshallers.fillBuiltInCollectionFrom(input, collectionToFill, descriptor, this::unmarshalShared, context);
    }

    private void fillBuiltInMapFrom(DataInputStream input, Map<?, ?> mapToFill, UnmarshallingContext context)
            throws UnmarshalException, IOException {
        builtInContainerMarshallers.fillBuiltInMapFrom(input, mapToFill, this::unmarshalShared, this::unmarshalShared, context);
    }

    private void fillGenericRefArrayFrom(DataInputStream input, Object[] array, UnmarshallingContext context)
            throws IOException, UnmarshalException {
        builtInContainerMarshallers.fillGenericRefArray(input, array, context);
    }

    @Nullable
    private Object readValue(DataInputStream input, ClassDescriptor descriptor, UnmarshallingContext context)
            throws IOException, UnmarshalException {
        if (isBuiltInNonContainer(descriptor)) {
            return builtInNonContainerMarshallers.readBuiltIn(descriptor, input, context);
        } else {
            throw new IllegalStateException("Cannot read an instance of " + descriptor.clazz() + ", this is a programmatic error");
        }
    }

    private Object applyReadResolveIfNeeded(Object object, ClassDescriptor descriptor) throws UnmarshalException {
        if (descriptor.hasReadResolve()) {
            return applyReadResolve(object, descriptor);
        } else {
            return object;
        }
    }

    private Object applyReadResolve(Object objectToResolve, ClassDescriptor descriptor) throws UnmarshalException {
        try {
            return descriptor.serializationMethods().readResolve(objectToResolve);
        } catch (SpecialMethodInvocationException e) {
            throw new UnmarshalException("Cannot apply readResolve()", e);
        }
    }

    private void throwIfNotDrained(InputStream dis) throws IOException, UnmarshalException {
        if (dis.available() > 0) {
            throw new UnmarshalException("After reading a value, " + dis.available() + " excessive byte(s) still remain");
        }
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
