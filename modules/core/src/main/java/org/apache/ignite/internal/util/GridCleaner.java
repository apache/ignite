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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import org.apache.ignite.IgniteException;

/**
 * The facade for Cleaner classes for compatibility between java 8 and 9.
 */
public class GridCleaner {
    /** Cleaner create method. */
    private static final Method cleanerCreateMtd;

    static {
        final String[] cleanerClsNames = {"sun.misc.Cleaner", "jdk.internal.ref.Cleaner"};

        final ClassLoader sysClsLdr = ClassLoader.getSystemClassLoader();

        Class<?> cleanerCls = null;

        for (String cleanerClsName : cleanerClsNames) {
            try {
                cleanerCls = Class.forName(cleanerClsName, true, sysClsLdr);

                break;
            }
            catch (ClassNotFoundException e) {
                // ignored;
            }
        }

        if (cleanerCls != null) {
            try {
                cleanerCreateMtd = cleanerCls.getMethod("create", Object.class, Runnable.class);
            }
            catch (NoSuchMethodException e) {
                throw new ExceptionInInitializerError(e);
            }
        }
        else
            throw new ExceptionInInitializerError("None of cleaner classes found: " + Arrays.toString(cleanerClsNames));
    }

    /**
     * @param obj Object.
     * @param run Run.
     */
    public static Object create(Object obj, Runnable run) {
        try {
            return cleanerCreateMtd.invoke(null, obj, run);
        }
        catch (IllegalAccessException | InvocationTargetException e) {
            throw new IgniteException(e);
        }
    }
}
