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

package org.apache.ignite.ml.environment.logging;

import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.math.Tracer;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * Simple logger printing to STD-out.
 */
public class ConsoleLogger implements MLLogger {
    /** Maximum Verbose level. */
    private final VerboseLevel maxVerboseLevel;
    /** Class name. */
    private final String className;

    /**
     * Creates an instance of ConsoleLogger.
     *
     * @param maxVerboseLevel Max verbose level.
     * @param clsName Class name.
     */
    private ConsoleLogger(VerboseLevel maxVerboseLevel, String clsName) {
        this.className = clsName;
        this.maxVerboseLevel = maxVerboseLevel;
    }

    /**
     * Returns an instance of ConsoleLogger factory.
     *
     * @param maxVerboseLevel Max verbose level.
     */
    public static Factory factory(VerboseLevel maxVerboseLevel) {
        return new Factory(maxVerboseLevel);
    }

    /** {@inheritDoc} */
    @Override public Vector log(Vector vector) {
        Tracer.showAscii(vector);
        return vector;
    }

    /** {@inheritDoc} */
    @Override public <K, V> Model<K, V> log(VerboseLevel verboseLevel, Model<K, V> mdl) {
        print(verboseLevel, mdl.toString(true));
        return mdl;
    }

    /** {@inheritDoc} */
    @Override public void log(VerboseLevel verboseLevel, String fmtStr, Object... params) {
        print(verboseLevel, String.format(fmtStr, params));
    }

    /**
     * Prints log line to STD-out.
     *
     * @param verboseLevel Verbose level.
     * @param line Line.
     */
    private void print(VerboseLevel verboseLevel, String line) {
        if (this.maxVerboseLevel.compareTo(verboseLevel) >= 0)
            System.out.println(String.format("%s [%s] %s", className, verboseLevel.name(), line));
    }

    /**
     * ConsoleLogger factory.
     */
    private static class Factory implements MLLogger.Factory {
        /** Max Verbose level. */
        private final VerboseLevel maxVerboseLevel;

        /**
         * Creates an instance of ConsoleLogger factory.
         *
         * @param maxVerboseLevel Max verbose level.
         */
        private Factory(VerboseLevel maxVerboseLevel) {
            this.maxVerboseLevel = maxVerboseLevel;
        }

        /** {@inheritDoc} */
        @Override public <T> MLLogger create(Class<T> targetCls) {
            return new ConsoleLogger(maxVerboseLevel, targetCls.getName());
        }
    }
}
