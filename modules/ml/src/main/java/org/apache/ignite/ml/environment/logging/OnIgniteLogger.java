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
    private final VerboseLevel currentVerboseLevel;
    private final IgniteLogger logger;
    private final IgniteLoggingLevel loggingLevel;

    public OnIgniteLogger(IgniteLogger logger,
        VerboseLevel currentVerboseLevel,
        IgniteLoggingLevel loggingLevel) {

        this.logger = logger;
        this.currentVerboseLevel = currentVerboseLevel;
        this.loggingLevel = loggingLevel;
    }

    @Override public Vector log(VerboseLevel level, Vector vector) {
        if (needLog(level))
            log(Formatters.getInstance().format(vector));

        return vector;
    }

    @Override public <K, V> Model<K, V> log(VerboseLevel level, Model<K, V> mdl) {
        if (needLog(level))
            log(Formatters.getInstance().format(mdl));

        return mdl;
    }

    @Override public void log(VerboseLevel level, String fmtStr, Object... params) {
        if (needLog(level))
            log(String.format(fmtStr, params));
    }

    private boolean needLog(VerboseLevel lvl) {
        return currentVerboseLevel.compareTo(lvl) >= 0;
    }

    private void log(String line) {
        switch (loggingLevel) {
            case INFO:
                logger.info(line);
                break;
            case TRACE:
                logger.trace(line);
                break;
            case DEBUG:
                logger.debug(line);
                break;
            case WARNING:
                logger.warning(line);
                break;
            case ERROR:
                logger.error(line);
                break;
        }
    }

    public static enum IgniteLoggingLevel {
        TRACE, INFO, DEBUG, WARNING, ERROR
    }
}
