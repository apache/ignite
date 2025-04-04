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

package org.apache.ignite.internal.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.ignite.IgniteCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 * Collection of utility methods used in 'ignite-commons' and throughout the system.
 */
public abstract class CommonsUtils {
    /** Sun-specific JDK constructor factory for objects that don't have empty constructor. */
    private static final Method CTOR_FACTORY;

    /** Sun JDK reflection factory. */
    private static final Object SUN_REFLECT_FACTORY;

    /** Public {@code java.lang.Object} no-argument constructor. */
    private static final Constructor OBJECT_CTOR;

    static {
        try {
            OBJECT_CTOR = Object.class.getConstructor();
        }
        catch (NoSuchMethodException e) {
            throw new AssertionError("Object class does not have empty constructor (is JDK corrupted?).", e);
        }

        // Constructor factory.
        Method ctorFac = null;
        Object refFac = null;

        try {
            Class<?> refFactoryCls = Class.forName("sun.reflect.ReflectionFactory");

            refFac = refFactoryCls.getMethod("getReflectionFactory").invoke(null);

            ctorFac = refFac.getClass().getMethod("newConstructorForSerialization", Class.class,
                Constructor.class);
        }
        catch (NoSuchMethodException | ClassNotFoundException | IllegalAccessException | InvocationTargetException ignored) {
            // No-op.
        }

        CTOR_FACTORY = ctorFac;
        SUN_REFLECT_FACTORY = refFac;
    }

    /**
     * Creates new instance of a class even if it does not have public constructor.
     *
     * @param cls Class to instantiate.
     * @return New instance of the class or {@code null} if empty constructor could not be assigned.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public static <T> T forceNewInstance(Class<?> cls) throws IgniteCheckedException {
        Constructor ctor = forceEmptyConstructor(cls);

        if (ctor == null)
            return null;

        boolean set = false;

        try {

            if (!ctor.isAccessible()) {
                ctor.setAccessible(true);

                set = true;
            }

            return (T)ctor.newInstance();
        }
        catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
            throw new IgniteCheckedException("Failed to create new instance for class: " + cls, e);
        }
        finally {
            if (set)
                ctor.setAccessible(false);
        }
    }

    /**
     * Gets empty constructor for class even if the class does not have empty constructor
     * declared. This method is guaranteed to work with SUN JDK and other JDKs still need
     * to be tested.
     *
     * @param cls Class to get empty constructor for.
     * @return Empty constructor if one could be found or {@code null} otherwise.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public static Constructor<?> forceEmptyConstructor(Class<?> cls) throws IgniteCheckedException {
        Constructor<?> ctor = null;

        try {
            return cls.getDeclaredConstructor();
        }
        catch (Exception ignore) {
            Method ctorFac = CTOR_FACTORY;
            Object sunRefFac = sunReflectionFactory();

            if (ctorFac != null && sunRefFac != null)
                try {
                    ctor = (Constructor)ctorFac.invoke(sunRefFac, cls, OBJECT_CTOR);
                }
                catch (IllegalAccessException | InvocationTargetException e) {
                    throw new IgniteCheckedException("Failed to get object constructor for class: " + cls, e);
                }
        }

        return ctor;
    }

    /**
     * SUN JDK specific reflection factory for objects without public constructor.
     *
     * @return Reflection factory for objects without public constructor.
     */
    @Nullable public static Object sunReflectionFactory() {
        return SUN_REFLECT_FACTORY;
    }

    /**
     * Returns a capacity that is sufficient to keep the map from being resized as
     * long as it grows no larger than expSize and the load factor is >= its
     * default (0.75).
     *
     * Copy pasted from guava. See com.google.common.collect.Maps#capacity(int)
     *
     * @param expSize Expected size of created map.
     * @return Capacity.
     */
    public static int capacity(int expSize) {
        if (expSize < 3)
            return expSize + 1;

        if (expSize < (1 << 30))
            return expSize + expSize / 3;

        return Integer.MAX_VALUE; // any large value
    }
}
