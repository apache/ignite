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
import org.apache.ignite.cache.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

/**
 * Pre-loads caches. Made callable just to conform common pattern.
 */
@GridInternal
public class VisorCachePreloadTask extends VisorOneNodeTask<Set<String>, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorCachesPreloadJob job(Set<String> arg) {
        return new VisorCachesPreloadJob(arg, debug);
    }

    /**
     * Job that preload caches.
     */
    private static class VisorCachesPreloadJob extends VisorJob<Set<String>, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Caches names.
         * @param debug Debug flag.
         */
        private VisorCachesPreloadJob(Set<String> arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Void run(Set<String> cacheNames) throws IgniteCheckedException {
            Collection<IgniteFuture<?>> futs = new ArrayList<>();

            for(GridCache c : g.cachesx()) {
                if (cacheNames.contains(c.name()))
                    futs.add(c.forceRepartition());
            }

            for (IgniteFuture f: futs)
                f.get();

            return null;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorCachesPreloadJob.class, this);
        }
    }
}
