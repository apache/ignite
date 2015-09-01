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

package org.apache.ignite.internal.processors.resource;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.lang.IgniteClosure;

/**
 * Collection of utility methods used in package for classes reflection.
 */
final class GridResourceUtils {
    /**
     * Ensure singleton.
     */
    private GridResourceUtils() {
        // No-op.
    }

    /**
     * Sets the field represented by this {@code field} object on the
     * specified object argument {@code target} to the specified new value {@code rsrc}.
     *
     * @param field Field where resource should be injected.
     * @param target Target object.
     * @param rsrc Resource object which should be injected in target object field.
     * @throws IgniteCheckedException Thrown if unable to inject resource.
     */
    @SuppressWarnings({"ErrorNotRethrown"})
    static void inject(Field field, Object target, Object rsrc) throws IgniteCheckedException {
        if (rsrc != null && !field.getType().isAssignableFrom(rsrc.getClass()))
            throw new IgniteCheckedException("Resource field is not assignable from the resource: " + rsrc.getClass());

        try {
            // Override default Java access check.
            field.setAccessible(true);

            field.set(target, rsrc);
        }
        catch (SecurityException | ExceptionInInitializerError | IllegalAccessException e) {
            throw new IgniteCheckedException("Failed to inject resource [field=" + field.getName() +
                ", target=" + target + ", rsrc=" + rsrc + ']', e);
        }
    }

    /**
     * Invokes the underlying method {@code mtd} represented by this
     * {@link Method} object, on the specified object {@code target}
     * with the specified parameter object {@code rsrc}.
     *
     * @param mtd Method which should be invoked to inject resource.
     * @param target Target object.
     * @param rsrc Resource object which should be injected.
     * @throws IgniteCheckedException Thrown if unable to inject resource.
     */
    @SuppressWarnings({"ErrorNotRethrown"})
    static void inject(Method mtd, Object target, Object rsrc) throws IgniteCheckedException {
        if (mtd.getParameterTypes().length != 1 ||
            (rsrc != null && !mtd.getParameterTypes()[0].isAssignableFrom(rsrc.getClass()))) {
            throw new IgniteCheckedException("Setter does not have single parameter of required type [type=" +
                rsrc.getClass().getName() + ", setter=" + mtd + ']');
        }

        try {
            mtd.setAccessible(true);

            mtd.invoke(target, rsrc);
        }
        catch (IllegalAccessException | ExceptionInInitializerError | InvocationTargetException e) {
            throw new IgniteCheckedException("Failed to inject resource [method=" + mtd.getName() +
                ", target=" + target + ", rsrc=" + rsrc + ']', e);
        }
    }

    /**
     * Checks if specified field requires recursive inspection to find resource annotations.
     *
     * @param f Field.
     * @return {@code true} if requires, {@code false} if doesn't.
     */
    static boolean mayRequireResources(Field f) {
        assert f != null;

        // Need to inspect anonymous classes, callable and runnable instances.
        return f.getName().startsWith("this$") || f.getName().startsWith("val$") ||
            Callable.class.isAssignableFrom(f.getType()) || Runnable.class.isAssignableFrom(f.getType()) ||
            IgniteClosure.class.isAssignableFrom(f.getType());
    }
}