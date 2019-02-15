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
