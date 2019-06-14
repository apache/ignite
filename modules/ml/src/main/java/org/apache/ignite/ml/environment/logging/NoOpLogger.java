/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.environment.logging;

import org.apache.ignite.ml.IgniteModel;
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
    @Override public <K, V> IgniteModel<K, V> log(VerboseLevel verboseLevel, IgniteModel<K, V> mdl) {
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
        private static final NoOpLogger NO_OP_LOGGER = new NoOpLogger();

        /** {@inheritDoc} */
        @Override public <T> MLLogger create(Class<T> targetCls) {
            return NO_OP_LOGGER;
        }
    }
}
