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

package org.apache.ignite.internal.benchmarks.jmh;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Base class for all JMH-related benchmarks.
 */
public abstract class JmhAbstractBenchmark {
    /**
     * Generate random integer value.
     *
     * @return Value.
     */
    protected static int randomInt() {
        return ThreadLocalRandom.current().nextInt();
    }

    /**
     * Generate random integer value.
     *
     * @param max Upper bound.
     * @return Value.
     */
    protected static int randomInt(int max) {
        return ThreadLocalRandom.current().nextInt(max);
    }

    /**
     * Get boolean property.
     *
     * @param name Name.
     * @return Value.
     */
    protected static boolean booleanProperty(String name) {
        return booleanProperty(name, false);
    }

    /**
     * Get boolean property.
     *
     * @param name Name.
     * @param dflt Default.
     * @return Value.
     */
    protected static boolean booleanProperty(String name, boolean dflt) {
        String val = property(name);

        return val != null ? Boolean.parseBoolean(val) : dflt;
    }

    /**
     * Get int property.
     *
     * @param name Name.
     * @return Value.
     */
    protected static int intProperty(String name) {
        return intProperty(name, 0);
    }

    /**
     * Get int property.
     *
     * @param name Name.
     * @param dflt Default value.
     * @return Value.
     */
    protected static int intProperty(String name, int dflt) {
        String val = property(name);

        return val != null ? Integer.parseInt(val) : dflt;
    }

    /**
     * Get string property.
     *
     * @param name Name.
     * @return Result.
     */
    protected static String stringProperty(String name) {
        return stringProperty(name, null);
    }

    /**
     * Get string property.
     *
     * @param name Name.
     * @param dflt Default value.
     * @return Result.
     */
    protected static String stringProperty(String name, String dflt) {
        String val = property(name);

        return val != null ? val : dflt;
    }

    /**
     * Get enum property.
     *
     * @param name Name.
     * @param cls Class.
     * @return Value.
     */
    @SuppressWarnings("unchecked")
    protected static <T> T enumProperty(String name, Class cls) {
        return enumProperty(name, cls, null);
    }

    /**
     * Get enum property.
     *
     * @param name Name.
     * @param cls Class.
     * @param dflt Default value.
     * @return Value.
     */
    @SuppressWarnings("unchecked")
    protected static <T> T enumProperty(String name, Class cls, T dflt) {
        String val = property(name);

        return val != null ? (T)Enum.valueOf(cls, val) : dflt;
    }

    /**
     * Get property's value.
     *
     * @param name Name.
     * @return Value.
     */
    private static String property(String name) {
        return System.getProperty(name);
    }
}
