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

package org.apache.ignite.internal.visor.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;

/**
 * Pre-loads caches. Made callable just to conform common pattern.
 */
@GridInternal
public class VisorCacheRebalanceTask extends VisorOneNodeTask<Set<String>, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorCachesRebalanceJob job(Set<String> arg) {
        return new VisorCachesRebalanceJob(arg, debug);
    }

    /**
     * Job that rebalance caches.
     */
    private static class VisorCachesRebalanceJob extends VisorJob<Set<String>, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Caches names.
         * @param debug Debug flag.
         */
        private VisorCachesRebalanceJob(Set<String> arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Void run(Set<String> cacheNames) {
            try {
                Collection<IgniteInternalFuture<?>> futs = new ArrayList<>();

                for (IgniteInternalCache c : ignite.cachesx()) {
                    if (cacheNames.contains(c.name()))
                        futs.add(c.rebalance());
                }

                for (IgniteInternalFuture f : futs)
                    f.get();

                return null;
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorCachesRebalanceJob.class, this);
        }
    }
}