/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
