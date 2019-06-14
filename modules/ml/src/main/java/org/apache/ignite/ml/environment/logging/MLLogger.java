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
 * Helper for ML-specific objects logging.
 */
public interface MLLogger {
    /**
     * Logging verbose level.
     */
    enum VerboseLevel {
        /** Disabled. */
        OFF,

        /** Low. */
        LOW,

        /** High. */
        HIGH
    }

    /**
     * Log vector.
     *
     * @param vector Vector.
     */
    public Vector log(Vector vector);

    /**
     * Log model according to toString method.
     *
     * @param verboseLevel Verbose level.
     * @param mdl Model.
     */
    public <K, V> IgniteModel<K,V> log(VerboseLevel verboseLevel, IgniteModel<K, V> mdl);

    /**
     * Log line with formatting.
     *
     * @param verboseLevel Verbose level.
     * @param fmtStr Format string.
     * @param params Params.
     */
    public void log(VerboseLevel verboseLevel, String fmtStr, Object... params);

    /**
     * MLLogger factory interface.
     */
    public static interface Factory {
        /**
         * Creates an instance of MLLogger for target class.
         *
         * @param targetCls For class.
         */
        public <T> MLLogger create(Class<T> targetCls);
    }
}
