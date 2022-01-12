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

import java.io.ObjectStreamClass;
import java.io.Serializable;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

/**
 * Instantiates {@link Serializable} classes using the mechanism defined by Java Serialization. That is,
 * for an {@link java.io.Externalizable}, its no-arg constructor is invoked. For a non-Externalizable {@link Serializable},
 * a new constructor is generated that invokes the no-arg constructor of the deepest non-serializable ancestor in the hierarchy.
 */
class SerializableInstantiation implements Instantiation {

    private static final MethodHandle STREAM_CLASS_NEW_INSTANCE;

    static {
        try {
            STREAM_CLASS_NEW_INSTANCE = streamClassNewInstanceMethodHandle();
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static MethodHandle streamClassNewInstanceMethodHandle() throws NoSuchMethodException, IllegalAccessException {
        MethodHandles.Lookup lookup = MethodHandles.privateLookupIn(ObjectStreamClass.class, MethodHandles.lookup());
        return lookup.findVirtual(ObjectStreamClass.class, "newInstance", MethodType.methodType(Object.class));
    }

    /** {@inheritDoc} */
    @Override
    public boolean supports(Class<?> objectClass) {
        return Serializable.class.isAssignableFrom(objectClass);
    }

    /** {@inheritDoc} */
    @Override
    public Object newInstance(Class<?> objectClass) throws InstantiationException {
        // Using the standard machinery (ObjectStreamClass) to instantiate an object to avoid generating excessive constructors
        // (as the standard machinery caches the constructors effectively).

        ObjectStreamClass desc = ObjectStreamClass.lookup(objectClass);

        // But as ObjectStreamClass#newInstance() is package-local, we have to resort to reflection/method handles magic.

        try {
            return STREAM_CLASS_NEW_INSTANCE.invokeExact(desc);
        } catch (Error e) {
            throw e;
        } catch (Throwable e) {
            throw new InstantiationException("Cannot instantiate", e);
        }
    }
}
