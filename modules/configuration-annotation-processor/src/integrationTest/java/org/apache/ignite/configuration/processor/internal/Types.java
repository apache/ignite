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

package org.apache.ignite.configuration.processor.internal;

/**
 * Type names for testing.
 */
public class Types {
    /** Java lang package name. */
    private static final String PKG_JAVA_LANG = "java.lang.";

    /** Integer. */
    public static final String INT = PKG_JAVA_LANG + "Integer";

    /** Long. */
    public static final String LONG = PKG_JAVA_LANG + "Long";

    /** String. */
    public static final String STRING = PKG_JAVA_LANG + "String";

    /** Double. */
    public static final String DOUBLE = PKG_JAVA_LANG + "Double";

    /**
     * Get type name by package name and class name.
     * @param packageName Package name.
     * @param className Class name.
     * @return Type name.
     */
    public static String typeName(String packageName, String className) {
        return String.format("%s.%s", packageName, className);
    }

}
