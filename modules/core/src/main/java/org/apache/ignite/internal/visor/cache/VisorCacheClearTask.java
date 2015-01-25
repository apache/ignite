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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.internal.processors.task.*;
import org.apache.ignite.internal.visor.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.util.*;

/**
 * Task that clears specified caches on specified node.
 */
@GridInternal
public class VisorCacheClearTask extends VisorOneNodeTask<Set<String>, Map<String, IgniteBiTuple<Integer, Integer>>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorCachesClearJob job(Set<String> arg) {
        return new VisorCachesClearJob(arg, debug);
    }

    /**
     * Job that clear specified caches.
     */
    private static class VisorCachesClearJob extends VisorJob<Set<String>, Map<String, IgniteBiTuple<Integer, Integer>>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job.
         *
         * @param arg Cache names to clear.
         * @param debug Debug flag.
         */
        private VisorCachesClearJob(Set<String> arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Map<String, IgniteBiTuple<Integer, Integer>> run(Set<String> arg) throws IgniteCheckedException {
            Map<String, IgniteBiTuple<Integer, Integer>> res = new HashMap<>();

            for(GridCache cache : g.cachesx()) {
                String cacheName = cache.name();

                if (arg.contains(cacheName)) {
                    Set keys = cache.keySet();

                    int before = keys.size(), after = before;

                    for (Object key : keys) {
                        if (cache.clearLocally(key))
                            after--;
                    }

                    res.put(cacheName, new IgniteBiTuple<>(before, after));
                }
            }

            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorCachesClearJob.class, this);
        }
    }
}
