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

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.environment.logging.formatter.MLFormatters;
import org.apache.ignite.ml.math.Tracer;
import org.apache.ignite.ml.math.Vector;

/**
 * MLLogger implementation based on IgniteLogger
 */
public class OnIgniteMLLogger implements MLLogger {
    /** Ignite logger instance. */
    private final IgniteLogger logger;

    /**
     * Creates an instance of OnIgniteMLLogger.
     *
     * @param logger Basic Logger.
     */
    private OnIgniteMLLogger(IgniteLogger logger) {
        this.logger = logger;
    }

    /**
     * Returns factory for OnIgniteLogger instantiating.
     *
     * @param rootLogger Root logger.
     */
    public static Factory factory(IgniteLogger rootLogger) {
        return new Factory(rootLogger);
    }

    /** {@inheritDoc} */
    @Override public Vector log(Vector vector) {
        Tracer.showAscii(vector, logger);
        return vector;
    }

    /** {@inheritDoc} */
    @Override public <K, V> Model<K, V> log(VerboseLevel verboseLevel, Model<K, V> mdl) {
        log(verboseLevel, MLFormatters.getInstance().format(mdl));
        return mdl;
    }

    /** {@inheritDoc} */
    @Override public void log(VerboseLevel verboseLevel, String fmtStr, Object... params) {
        log(verboseLevel, String.format(fmtStr, params));
    }

    /**
     * Log line.
     *
     * @param verboseLevel Verbose level.
     * @param line Line.
     */
    private void log(VerboseLevel verboseLevel, String line) {
        switch (verboseLevel) {
            case MIN:
                logger.info(line);
                break;
            case MID:
                logger.debug(line);
                break;
            case MAX:
                logger.trace(line);
                break;
        }
    }

    /**
     * OnIgniteMLLogger factory.
     */
    private static class Factory implements MLLogger.Factory {
        /** Root logger. */
        private IgniteLogger rootLogger;

        /**
         * Creates an instance of factory.
         *
         * @param rootLogger Root logger.
         */
        public Factory(IgniteLogger rootLogger) {
            this.rootLogger = rootLogger;
        }

        /** {@inheritDoc} */
        @Override public <T> MLLogger create(Class<T> targetCls) {
            return new OnIgniteMLLogger(rootLogger.getLogger(targetCls));
        }
    }
}
