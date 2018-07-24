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
 * Helper for ML-specific objects logging.
 */
public interface MLLogger {
    /**
     * Logging verbose level.
     */
    enum VerboseLevel {
        OFF, LOW, HIGH
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
    public <K, V> Model<K,V> log(VerboseLevel verboseLevel, Model<K, V> mdl);

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
