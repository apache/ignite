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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

/**
 * {@link FieldAccessor} implementation.
 */
class FieldAccessorImpl implements FieldAccessor {
    private final Field field;
    private final VarHandle varHandle;

    FieldAccessorImpl(FieldDescriptor descriptor) {
        field = findField(descriptor);
        field.setAccessible(true);

        varHandle = varHandleFrom(field);
    }

    private static Field findField(FieldDescriptor fieldDescriptor) {
        try {
            return fieldDescriptor.declaringClass().getDeclaredField(fieldDescriptor.name());
        } catch (NoSuchFieldException e) {
            throw new ReflectionException("Cannot find field", e);
        }
    }

    private static VarHandle varHandleFrom(Field field) {
        try {
            MethodHandles.Lookup lookup = MethodHandles.privateLookupIn(field.getDeclaringClass(), MethodHandles.lookup());
            return lookup.unreflectVarHandle(field);
        } catch (ReflectiveOperationException e) {
            throw new ReflectionException("Cannot get a field VarHandle", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public Object get(Object target) {
        return varHandle.get(target);
    }

    /** {@inheritDoc} */
    @Override
    public void set(Object target, Object fieldValue) {
        if (isFieldFinal()) {
            setViaField(target, fieldValue);
        } else {
            varHandle.set(target, fieldValue);
        }
    }

    private boolean isFieldFinal() {
        return Modifier.isFinal(field.getModifiers());
    }

    private void setViaField(Object target, Object fieldValue) {
        try {
            field.set(target, fieldValue);
        } catch (IllegalAccessException e) {
            throw new ReflectionException("Cannot set a value", e);
        }
    }
}
