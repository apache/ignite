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
import org.apache.ignite.ml.environment.logging.formatter.Formatters;
import org.apache.ignite.ml.math.Vector;

public class OnIgniteLogger implements MLLogger {
    private final IgniteLogger logger;

    private OnIgniteLogger(IgniteLogger logger) {
        this.logger = logger;
    }

    public static Factory factory(IgniteLogger rootLogger) {
        return new Factory(rootLogger);
    }

    @Override public Vector log(VerboseLevel verboseLevel, Vector vector) {
        log(verboseLevel, Formatters.getInstance().format(vector));
        return vector;
    }

    @Override public <K, V> Model<K, V> log(VerboseLevel verboseLevel, Model<K, V> mdl) {
        log(verboseLevel, Formatters.getInstance().format(mdl));
        return mdl;
    }

    @Override public void log(VerboseLevel verboseLevel, String fmtStr, Object... params) {
        log(verboseLevel, String.format(fmtStr, params));
    }

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

    private static class Factory implements MLLogger.Factory {
        private IgniteLogger rootLogger;

        public Factory(IgniteLogger rootLogger) {
            this.rootLogger = rootLogger;
        }

        @Override public <T> MLLogger create(Class<T> forClass) {
            return new OnIgniteLogger(rootLogger.getLogger(forClass));
        }
    }
}
