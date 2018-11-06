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
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * MLLogger implementation skipping all logs.
 */
public class NoOpLogger implements MLLogger {
    /** Factory. */
    private static final Factory FACTORY = new Factory();

    /**
     * Returns NoOpLogger factory.
     */
    public static Factory factory() {
        return FACTORY;
    }

    /** {@inheritDoc} */
    @Override public Vector log(Vector vector) {
        return vector;
    }

    /** {@inheritDoc} */
    @Override public <K, V> Model<K, V> log(VerboseLevel verboseLevel, Model<K, V> mdl) {
        return mdl;
    }

    /** {@inheritDoc} */
    @Override public void log(VerboseLevel verboseLevel, String fmtStr, Object... params) {

    }

    /**
     * NoOpLogger factory.
     */
    private static class Factory implements MLLogger.Factory {
        /** NoOpLogger instance. */
        private final static NoOpLogger NO_OP_LOGGER = new NoOpLogger();

        /** {@inheritDoc} */
        @Override public <T> MLLogger create(Class<T> targetCls) {
            return NO_OP_LOGGER;
        }
    }
}
