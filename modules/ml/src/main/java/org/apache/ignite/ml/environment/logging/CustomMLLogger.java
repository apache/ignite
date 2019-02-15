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

package org.apache.ignite.ml.environment.logging;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.math.Tracer;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * MLLogger implementation based on IgniteLogger.
 */
public class CustomMLLogger implements MLLogger {
    /** Ignite logger instance. */
    private final IgniteLogger log;

    /**
     * Creates an instance of CustomMLLogger.
     *
     * @param log Basic Logger.
     */
    private CustomMLLogger(IgniteLogger log) {
        this.log = log;
    }

    /**
     * Returns factory for OnIgniteLogger instantiating.
     *
     * @param rootLog Root logger.
     */
    public static Factory factory(IgniteLogger rootLog) {
        return new Factory(rootLog);
    }

    /** {@inheritDoc} */
    @Override public Vector log(Vector vector) {
        Tracer.showAscii(vector, log);
        return vector;
    }

    /** {@inheritDoc} */
    @Override public <K, V> Model<K, V> log(VerboseLevel verboseLevel, Model<K, V> mdl) {
        log(verboseLevel, mdl.toString(true));
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
            case LOW:
                log.info(line);
                break;
            case HIGH:
                log.debug(line);
                break;
        }
    }

    /**
     * CustomMLLogger factory.
     */
    private static class Factory implements MLLogger.Factory {
        /** Root logger. */
        private IgniteLogger rootLog;

        /**
         * Creates an instance of factory.
         *
         * @param rootLog Root logger.
         */
        public Factory(IgniteLogger rootLog) {
            this.rootLog = rootLog;
        }

        /** {@inheritDoc} */
        @Override public <T> MLLogger create(Class<T> targetCls) {
            return new CustomMLLogger(rootLog.getLogger(targetCls));
        }
    }
}
