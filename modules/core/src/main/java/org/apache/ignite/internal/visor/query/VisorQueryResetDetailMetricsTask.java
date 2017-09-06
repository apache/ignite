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

package org.apache.ignite.internal.visor.query;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;

/**
 * Reset query detail metrics.
 */
@GridInternal
public class VisorQueryResetDetailMetricsTask extends VisorOneNodeTask<Void, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorCacheResetQueryDetailMetricsJob job(Void arg) {
        return new VisorCacheResetQueryDetailMetricsJob(arg, debug);
    }

    /**
     * Job that reset query detail metrics.
     */
    private static class VisorCacheResetQueryDetailMetricsJob extends VisorJob<Void, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Task argument.
         * @param debug Debug flag.
         */
        private VisorCacheResetQueryDetailMetricsJob(Void arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Void run(Void arg) {
            for (String cacheName : ignite.cacheNames()) {
                IgniteCache cache = ignite.cache(cacheName);

                if (cache == null)
                    throw new IllegalStateException("Failed to find cache for name: " + cacheName);

                cache.resetQueryDetailMetrics();
            }

            return null;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorCacheResetQueryDetailMetricsJob.class, this);
        }
    }
}
