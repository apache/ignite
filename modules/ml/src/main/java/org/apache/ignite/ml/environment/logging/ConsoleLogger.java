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
import org.apache.ignite.ml.environment.logging.formatter.Formatters;
import org.apache.ignite.ml.math.Vector;

public class ConsoleLogger implements MLLogger {
    private final VerboseLevel verboseLevel;
    private final String className;

    private ConsoleLogger(VerboseLevel verboseLevel, String className) {
        this.className = className;
        this.verboseLevel = verboseLevel;
    }

    public static Factory factory(VerboseLevel verboseLevel) {
        return new Factory(verboseLevel);
    }

    @Override public Vector log(VerboseLevel verboseLevel, Vector vector) {
        print(verboseLevel, Formatters.getInstance().format(vector));
        return vector;
    }

    @Override public <K, V> Model<K, V> log(VerboseLevel verboseLevel, Model<K, V> mdl) {
        print(verboseLevel, Formatters.getInstance().format(mdl));
        return mdl;
    }

    @Override public void log(VerboseLevel verboseLevel, String fmtStr, Object... params) {
        print(verboseLevel, String.format(fmtStr, params));
    }

    private void print(VerboseLevel verboseLevel, String line) {
        if (this.verboseLevel.compareTo(verboseLevel) >= 0)
            System.out.println(String.format("%s [%s] %s", className, verboseLevel.name(), line));
    }

    private static class Factory implements MLLogger.Factory {
        private final VerboseLevel verboseLevel;

        private Factory(VerboseLevel verboseLevel) {
            this.verboseLevel = verboseLevel;
        }

        @Override public <T> MLLogger create(Class<T> forClass) {
            return new ConsoleLogger(verboseLevel, forClass.getName());
        }
    }
}
