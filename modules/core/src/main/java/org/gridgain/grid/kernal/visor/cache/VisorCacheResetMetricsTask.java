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

package org.gridgain.grid.kernal.visor.cache;

import org.apache.ignite.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.*;
import org.gridgain.grid.util.typedef.internal.*;

/**
 * Reset compute grid metrics.
 */
@GridInternal
public class VisorCacheResetMetricsTask extends VisorOneNodeTask<String, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorCacheResetMetricsJob job(String arg) {
        return new VisorCacheResetMetricsJob(arg, debug);
    }

    /**
     * Job that reset cache metrics.
     */
    private static class VisorCacheResetMetricsJob extends VisorJob<String, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Cache name to reset metrics for.
         * @param debug Debug flag.
         */
        private VisorCacheResetMetricsJob(String arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Void run(String cacheName) throws IgniteCheckedException {
            GridCache cache = g.cachex(cacheName);

            if (cache != null)
                cache.resetMetrics();

            return null;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorCacheResetMetricsJob.class, this);
        }
    }
}
