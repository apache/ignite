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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.util.Objects;
import org.apache.ignite.internal.network.serialization.ClassDescriptor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Encapsulates special serialization methods like writeReplace()/readResolve() for convenient invocation.
 */
class SpecialSerializationMethods {
    /** MethodHandle that can be used to invoke writeReplace() on the target class. */
    @Nullable
    private final MethodHandle writeReplaceHandle;

    /** MethodHandle that can be used to invoke readResolve() on the target class. */
    @Nullable
    private final MethodHandle readResolveHandle;

    /**
     * Creates a new instance from the provided descriptor.
     *
     * @param descriptor class descriptor on which class to operate
     */
    public SpecialSerializationMethods(ClassDescriptor descriptor) {
        writeReplaceHandle = descriptor.hasWriteReplace() ? writeReplaceHandle(descriptor) : null;
        readResolveHandle = descriptor.hasReadResolve() ? readResolveHandle(descriptor) : null;
    }

    private static MethodHandle writeReplaceHandle(ClassDescriptor descriptor) {
        Method writeReplaceMethod = findWriteReplaceMethod(descriptor);

        try {
            return MethodHandles.lookup()
                        .unreflect(writeReplaceMethod)
                        .asType(MethodType.methodType(Object.class, Object.class));
        } catch (IllegalAccessException e) {
            throw new ReflectionException("writeReplace() cannot be unreflected", e);
        }
    }

    private static Method findWriteReplaceMethod(ClassDescriptor descriptor) {
        Method writeReplaceMethod;
        try {
            writeReplaceMethod = descriptor.clazz().getDeclaredMethod("writeReplace");
        } catch (NoSuchMethodException e) {
            throw new ReflectionException("writeReplace() was not found on " + descriptor.clazz()
                    + " even though the descriptor says the class has the method", e);
        }

        writeReplaceMethod.setAccessible(true);

        return writeReplaceMethod;
    }

    private static MethodHandle readResolveHandle(ClassDescriptor descriptor) {
        Method readResolveMethod = findReadResolveMethod(descriptor);

        try {
            return MethodHandles.lookup()
                    .unreflect(readResolveMethod)
                    .asType(MethodType.methodType(Object.class, Object.class));
        } catch (IllegalAccessException e) {
            throw new ReflectionException("readResolve() cannot be unreflected", e);
        }
    }

    @NotNull
    private static Method findReadResolveMethod(ClassDescriptor descriptor) {
        Method readResolveMethod;
        try {
            readResolveMethod = descriptor.clazz().getDeclaredMethod("readResolve");
        } catch (NoSuchMethodException e) {
            throw new ReflectionException("readResolve() was not found on " + descriptor.clazz()
                    + " even though the descriptor says the class has the method", e);
        }

        readResolveMethod.setAccessible(true);

        return readResolveMethod;
    }

    /**
     * Invokes {@code writeReplace()} on the target object. Should only be used if the target descriptor supports
     * the method (that is, its serialization type is
     * {@link org.apache.ignite.internal.network.serialization.SerializationType#SERIALIZABLE} or
     * {@link org.apache.ignite.internal.network.serialization.SerializationType#EXTERNALIZABLE} and the class
     * actually has writeReplace() method).
     * If any of these conditions fail, a {@link NullPointerException} will be thrown.
     *
     * @param object target object on which to invoke the method
     * @return invocation result
     * @throws MarshalException if the invocation fails
     */
    public Object writeReplace(Object object) throws MarshalException {
        Objects.requireNonNull(writeReplaceHandle);

        try {
            return writeReplaceHandle.invokeExact(object);
        } catch (Error e) {
            throw e;
        } catch (Throwable e) {
            throw new MarshalException("writeReplace() invocation failed on " + object, e);
        }
    }

    /**
     * Invokes {@code readResolve()} on the target object. Should only be used if the target descriptor supports
     * the method (that is, its serialization type is
     * {@link org.apache.ignite.internal.network.serialization.SerializationType#SERIALIZABLE} or
     * {@link org.apache.ignite.internal.network.serialization.SerializationType#EXTERNALIZABLE} and the class
     * actually has readResolve() method).
     * If any of these conditions fail, a {@link NullPointerException} will be thrown.
     *
     * @param object target object on which to invoke the method
     * @return invocation result
     * @throws UnmarshalException if the invocation fails
     */
    public Object readResolve(Object object) throws UnmarshalException {
        Objects.requireNonNull(readResolveHandle);

        try {
            return readResolveHandle.invokeExact(object);
        } catch (Error e) {
            throw e;
        } catch (Throwable e) {
            throw new UnmarshalException("readResolve() invocation failed on " + object, e);
        }
    }
}
