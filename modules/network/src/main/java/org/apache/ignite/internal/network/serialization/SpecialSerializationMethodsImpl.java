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

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Objects;
import org.jetbrains.annotations.Nullable;

/**
 * Encapsulates special serialization methods like writeReplace()/readResolve() for convenient invocation.
 */
class SpecialSerializationMethodsImpl implements SpecialSerializationMethods {
    /** MethodHandle that can be used to invoke writeReplace() on the target class. */
    @Nullable
    private final MethodHandle writeReplaceHandle;

    /** MethodHandle that can be used to invoke readResolve() on the target class. */
    @Nullable
    private final MethodHandle readResolveHandle;

    /** MethodHandle that can be used to invoke writeObject() on the target class. */
    @Nullable
    private final MethodHandle writeObjectHandle;

    /** MethodHandle that can be used to invoke readObject() on the target class. */
    @Nullable
    private final MethodHandle readObjectHandle;

    /**
     * Creates a new instance from the provided descriptor.
     *
     * @param descriptor class descriptor on which class to operate
     */
    public SpecialSerializationMethodsImpl(ClassDescriptor descriptor) {
        writeReplaceHandle = descriptor.hasWriteReplace() ? writeReplaceHandle(descriptor) : null;
        readResolveHandle = descriptor.hasReadResolve() ? readResolveHandle(descriptor) : null;
        writeObjectHandle = descriptor.hasSerializationOverride() ? writeObjectHandle(descriptor) : null;
        readObjectHandle = descriptor.hasSerializationOverride() ? readObjectHandle(descriptor) : null;
    }

    private static MethodHandle writeReplaceHandle(ClassDescriptor descriptor) {
        try {
            return MethodHandles.privateLookupIn(descriptor.clazz(), MethodHandles.lookup())
                    .findVirtual(descriptor.clazz(), "writeReplace", MethodType.methodType(Object.class))
                    .asType(MethodType.methodType(Object.class, Object.class));
        } catch (ReflectiveOperationException e) {
            throw new ReflectionException("Cannot find writeReplace() in " + descriptor.clazz(), e);
        }
    }

    private static MethodHandle readResolveHandle(ClassDescriptor descriptor) {
        try {
            return MethodHandles.privateLookupIn(descriptor.clazz(), MethodHandles.lookup())
                    .findVirtual(descriptor.clazz(), "readResolve", MethodType.methodType(Object.class))
                    .asType(MethodType.methodType(Object.class, Object.class));
        } catch (ReflectiveOperationException e) {
            throw new ReflectionException("Cannot find readResolve() in " + descriptor.clazz(), e);
        }
    }

    private static MethodHandle writeObjectHandle(ClassDescriptor descriptor) {
        try {
            return MethodHandles.privateLookupIn(descriptor.clazz(), MethodHandles.lookup())
                    .findVirtual(descriptor.clazz(), "writeObject", MethodType.methodType(void.class, ObjectOutputStream.class))
                    .asType(MethodType.methodType(void.class, Object.class, ObjectOutputStream.class));
        } catch (ReflectiveOperationException e) {
            throw new ReflectionException("Cannot find writeObject() in " + descriptor.clazz(), e);
        }
    }

    private static MethodHandle readObjectHandle(ClassDescriptor descriptor) {
        try {
            return MethodHandles.privateLookupIn(descriptor.clazz(), MethodHandles.lookup())
                    .findVirtual(descriptor.clazz(), "readObject", MethodType.methodType(void.class, ObjectInputStream.class))
                    .asType(MethodType.methodType(void.class, Object.class, ObjectInputStream.class));
        } catch (ReflectiveOperationException e) {
            throw new ReflectionException("Cannot find readObject() in " + descriptor.clazz(), e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public Object writeReplace(Object object) throws SpecialMethodInvocationException {
        Objects.requireNonNull(writeReplaceHandle);

        try {
            return writeReplaceHandle.invokeExact(object);
        } catch (Error e) {
            throw e;
        } catch (Throwable e) {
            throw new SpecialMethodInvocationException("writeReplace() invocation failed on " + object, e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public Object readResolve(Object object) throws SpecialMethodInvocationException {
        Objects.requireNonNull(readResolveHandle);

        try {
            return readResolveHandle.invokeExact(object);
        } catch (Error e) {
            throw e;
        } catch (Throwable e) {
            throw new SpecialMethodInvocationException("readResolve() invocation failed on " + object, e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void writeObject(Object object, ObjectOutputStream stream) throws SpecialMethodInvocationException {
        Objects.requireNonNull(writeObjectHandle);

        try {
            writeObjectHandle.invokeExact(object, stream);
        } catch (Error e) {
            throw e;
        } catch (Throwable e) {
            throw new SpecialMethodInvocationException("writeObject() invocation failed on " + object, e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void readObject(Object object, ObjectInputStream stream) throws SpecialMethodInvocationException {
        Objects.requireNonNull(readObjectHandle);

        try {
            readObjectHandle.invokeExact(object, stream);
        } catch (Error e) {
            throw e;
        } catch (Throwable e) {
            throw new SpecialMethodInvocationException("readObject() invocation failed on " + object, e);
        }
    }
}
