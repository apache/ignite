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

package org.gridgain.grid.kernal.visor.streamer;

import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.*;
import org.gridgain.grid.util.typedef.internal.*;

import static org.gridgain.grid.kernal.visor.util.VisorTaskUtils.*;

/**
 * Task for reset metrics for specified streamer.
 */
@GridInternal
public class VisorStreamerMetricsResetTask extends VisorOneNodeTask<String, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorStreamerMetricsResetJob job(String arg) {
        return new VisorStreamerMetricsResetJob(arg, debug);
    }

    /**
     * Job that reset streamer metrics.
     */
    private static class VisorStreamerMetricsResetJob extends VisorJob<String, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Streamer name.
         * @param debug Debug flag.
         */
        private VisorStreamerMetricsResetJob(String arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Void run(String streamerName) throws IgniteCheckedException {
            try {
                IgniteStreamer streamer = g.streamer(streamerName);

                streamer.resetMetrics();

                return null;
            }
            catch (IllegalArgumentException iae) {
                throw new IgniteCheckedException("Failed to reset metrics for streamer: " + escapeName(streamerName) +
                    " on node: " + g.localNode().id(), iae);
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorStreamerMetricsResetJob.class, this);
        }
    }
}
