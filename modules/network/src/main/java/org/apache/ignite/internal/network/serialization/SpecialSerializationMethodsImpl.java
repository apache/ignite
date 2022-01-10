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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
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

    /**
     * Creates a new instance from the provided descriptor.
     *
     * @param descriptor class descriptor on which class to operate
     */
    public SpecialSerializationMethodsImpl(ClassDescriptor descriptor) {
        writeReplaceHandle = descriptor.hasWriteReplace() ? writeReplaceHandle(descriptor) : null;
        readResolveHandle = descriptor.hasReadResolve() ? readResolveHandle(descriptor) : null;
    }

    private static MethodHandle writeReplaceHandle(ClassDescriptor descriptor) {
        Method writeReplaceMethod = findWriteReplaceMethod(descriptor);

        return unreflect(writeReplaceMethod, MethodType.methodType(Object.class, Object.class), descriptor);
    }

    private static MethodHandle unreflect(Method method, MethodType methodType, ClassDescriptor descriptor) {
        try {
            return MethodHandles.privateLookupIn(descriptor.clazz(), MethodHandles.lookup())
                        .unreflect(method)
                        .asType(methodType);
        } catch (IllegalAccessException e) {
            throw new ReflectionException("Cannot unreflect", e);
        }
    }

    private static Method findWriteReplaceMethod(ClassDescriptor descriptor) {
        try {
            return descriptor.clazz().getDeclaredMethod("writeReplace");
        } catch (NoSuchMethodException e) {
            throw new ReflectionException("writeReplace() was not found on " + descriptor.clazz()
                    + " even though the descriptor says the class has the method", e);
        }
    }

    private static MethodHandle readResolveHandle(ClassDescriptor descriptor) {
        Method readResolveMethod = findReadResolveMethod(descriptor);

        return unreflect(readResolveMethod, MethodType.methodType(Object.class, Object.class), descriptor);
    }

    private static Method findReadResolveMethod(ClassDescriptor descriptor) {
        try {
            return descriptor.clazz().getDeclaredMethod("readResolve");
        } catch (NoSuchMethodException e) {
            throw new ReflectionException("readResolve() was not found on " + descriptor.clazz()
                    + " even though the descriptor says the class has the method", e);
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
}
