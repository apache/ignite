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

import java.lang.reflect.Method;
import java.util.Arrays;
import org.apache.ignite.IgniteException;

/**
 * The facade for Cleaner classes for compatibility between java 8 and 9.
 */
public class GridCleaner {
    /** Pre-java9 cleaner class name. */
    private static final String PRE_JAVA9_CLEANER_CLASS_NAME = "sun.misc.Cleaner";

    /** Java9 cleaner class name. */
    private static final String JAVA9_CLEANER_CLASS_NAME = "java.lang.ref.Cleaner";

    /** Cleaner class. */
    private static final Class<?> cls;

    /** Cleaner object. */
    private static final Object instance;

    /** Cleaner register method. */
    private static final Method initMtd;

    static {
        cls = findCleanerClass();

        try {
            String mtdName;

            if (JAVA9_CLEANER_CLASS_NAME.equals(cls.getName())) {
                instance = cls.getMethod("create").invoke(null);

                mtdName = "register";
            }
            else {
                instance = null;

                mtdName = "create";
            }

            initMtd = cls.getMethod(mtdName, Object.class, Runnable.class);
        }
        catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     *
     */
    private static Class<?> findCleanerClass() {
        String[] clsNames = {PRE_JAVA9_CLEANER_CLASS_NAME, JAVA9_CLEANER_CLASS_NAME};

        ClassLoader clsLdr = ClassLoader.getSystemClassLoader();

        for (String clsName : clsNames) {
            try {
                return Class.forName(clsName, true, clsLdr);
            }
            catch (ClassNotFoundException e) {
                // ignored;
            }
        }

        throw new IllegalStateException("None of cleaner classes found: " + Arrays.toString(clsNames));
    }

    /**
     * @param obj Object.
     * @param act Action.
     */
    public static Object create(Object obj, Runnable act) {
        try {
            return initMtd.invoke(instance, obj, act);
        }
        catch (ReflectiveOperationException e) {
            throw new IgniteException(e);
        }
    }
}
