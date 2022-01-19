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
import java.lang.reflect.Method;
import java.util.Objects;
import org.jetbrains.annotations.Nullable;

/**
 * Encapsulates special serialization methods like writeReplace()/readResolve() and so on for convenient invocation.
 */
class SpecialSerializationMethodsImpl implements SpecialSerializationMethods {
    /** Method that can be used to invoke writeReplace() on the target class. */
    @Nullable
    private final Method writeReplace;

    /** Method that can be used to invoke readResolve() on the target class. */
    @Nullable
    private final Method readResolve;

    /** Method that can be used to invoke writeObject() on the target class. */
    @Nullable
    private final Method writeObject;

    /** Method that can be used to invoke readObject() on the target class. */
    @Nullable
    private final Method readObject;

    /**
     * Creates a new instance from the provided descriptor.
     *
     * @param descriptor class descriptor on which class to operate
     */
    public SpecialSerializationMethodsImpl(ClassDescriptor descriptor) {
        writeReplace = descriptor.hasWriteReplace() ? writeReplaceInvoker(descriptor) : null;
        readResolve = descriptor.hasReadResolve() ? readResolveInvoker(descriptor) : null;
        writeObject = descriptor.hasWriteObject() ? writeObjectInvoker(descriptor) : null;
        readObject = descriptor.hasReadObject() ? readObjectInvoker(descriptor) : null;
    }

    private static Method writeReplaceInvoker(ClassDescriptor descriptor) {
        try {
            Method method = descriptor.clazz().getDeclaredMethod("writeReplace");
            method.setAccessible(true);
            return method;
        } catch (ReflectiveOperationException e) {
            throw new ReflectionException("Cannot find writeReplace() in " + descriptor.clazz(), e);
        }
    }

    private static Method readResolveInvoker(ClassDescriptor descriptor) {
        try {
            Method method = descriptor.clazz().getDeclaredMethod("readResolve");
            method.setAccessible(true);
            return method;
        } catch (ReflectiveOperationException e) {
            throw new ReflectionException("Cannot find readResolve() in " + descriptor.clazz(), e);
        }
    }

    private static Method writeObjectInvoker(ClassDescriptor descriptor) {
        try {
            Method method = descriptor.clazz().getDeclaredMethod("writeObject", ObjectOutputStream.class);
            method.setAccessible(true);
            return method;
        } catch (ReflectiveOperationException e) {
            throw new ReflectionException("Cannot find writeObject() in " + descriptor.clazz(), e);
        }
    }

    private static Method readObjectInvoker(ClassDescriptor descriptor) {
        try {
            Method method = descriptor.clazz().getDeclaredMethod("readObject", ObjectInputStream.class);
            method.setAccessible(true);
            return method;
        } catch (ReflectiveOperationException e) {
            throw new ReflectionException("Cannot find readObject() in " + descriptor.clazz(), e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public Object writeReplace(Object object) throws SpecialMethodInvocationException {
        Objects.requireNonNull(writeReplace);

        try {
            return writeReplace.invoke(object, (Object[]) null);
        } catch (ReflectiveOperationException e) {
            throw new SpecialMethodInvocationException("writeReplace() invocation failed on " + object, e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public Object readResolve(Object object) throws SpecialMethodInvocationException {
        Objects.requireNonNull(readResolve);

        try {
            return readResolve.invoke(object, (Object[]) null);
        } catch (ReflectiveOperationException e) {
            throw new SpecialMethodInvocationException("readResolve() invocation failed on " + object, e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void writeObject(Object object, ObjectOutputStream stream) throws SpecialMethodInvocationException {
        Objects.requireNonNull(writeObject);

        try {
            writeObject.invoke(object, stream);
        } catch (ReflectiveOperationException e) {
            throw new SpecialMethodInvocationException("writeObject() invocation failed on " + object, e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void readObject(Object object, ObjectInputStream stream) throws SpecialMethodInvocationException {
        Objects.requireNonNull(readObject);

        try {
            readObject.invoke(object, stream);
        } catch (ReflectiveOperationException e) {
            throw new SpecialMethodInvocationException("readObject() invocation failed on " + object, e);
        }
    }
}
