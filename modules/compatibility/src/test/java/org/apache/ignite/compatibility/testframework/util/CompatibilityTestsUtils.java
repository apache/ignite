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

package org.apache.ignite.compatibility.testframework.util;

import java.io.File;
import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLClassLoader;
import org.jetbrains.annotations.Nullable;

/**
 * Utility class for compatibility tests.
 *
 * May contain code duplication because of using a newly added code in the 'core' module which absent in previous
 * releases and which should be included in the classpath of separate JVM process.
 */
@SuppressWarnings("Duplicates")
public class CompatibilityTestsUtils {
    /** Empty URL array. */
    private static final URL[] EMPTY_URL_ARR = new URL[0];

    /**
     * Builtin class loader class.
     *
     * Note: needs for compatibility with Java 9.
     */
    private static final Class bltClsLdrCls = defaultClassLoaderClass();

    /**
     * Url class loader field.
     *
     * Note: needs for compatibility with Java 9.
     */
    private static final Field urlClsLdrField = urlClassLoaderField();

    /**
     * Returns URLs of class loader
     *
     * @param clsLdr Class loader.
     */
    public static URL[] classLoaderUrls(ClassLoader clsLdr) {
        if (clsLdr == null)
            return EMPTY_URL_ARR;
        else if (clsLdr instanceof URLClassLoader)
            return ((URLClassLoader)clsLdr).getURLs();
        else if (bltClsLdrCls != null && urlClsLdrField != null && bltClsLdrCls.isAssignableFrom(clsLdr.getClass())) {
            try {
                return ((URLClassLoader)urlClsLdrField.get(clsLdr)).getURLs();
            }
            catch (IllegalAccessException e) {
                return EMPTY_URL_ARR;
            }
        }
        else
            return EMPTY_URL_ARR;
    }

    /** */
    @Nullable private static Class defaultClassLoaderClass() {
        try {
            return Class.forName("jdk.internal.loader.BuiltinClassLoader");
        }
        catch (ClassNotFoundException e) {
            return null;
        }
    }

    /** */
    @Nullable private static Field urlClassLoaderField() {
        try {
            Class cls = defaultClassLoaderClass();

            return cls == null ? null : cls.getDeclaredField("ucp");
        }
        catch (NoSuchFieldException e) {
            return null;
        }
    }

    /**
     * Checks if the given directory is empty.
     *
     * @param dir Directory to check.
     * @return {@code true} if the given directory is empty or doesn't exist, otherwise {@code false}.
     */
    @SuppressWarnings("ConstantConditions")
    public static boolean isDirectoryEmpty(File dir) {
        return !dir.exists() || (dir.isDirectory() && dir.list().length == 0);
    }
}
