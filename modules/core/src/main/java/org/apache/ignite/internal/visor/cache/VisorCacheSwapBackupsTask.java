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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.cache.Cache;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 * Task for swapping backup cache entries.
 */
@GridInternal
public class VisorCacheSwapBackupsTask extends VisorOneNodeTask<Set<String>, Map<String,
    IgniteBiTuple<Integer, Integer>>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorCachesSwapBackupsJob job(Set<String> names) {
        return new VisorCachesSwapBackupsJob(names, debug);
    }

    /**
     * Job that swap backups.
     */
    private static class VisorCachesSwapBackupsJob extends VisorJob<Set<String>, Map<String,
        IgniteBiTuple<Integer, Integer>>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @IgniteInstanceResource
        protected IgniteEx g;

        /**
         * Create job with specified argument.
         *
         * @param names Job argument.
         * @param debug Debug flag.
         */
        private VisorCachesSwapBackupsJob(Set<String> names, boolean debug) {
            super(names, debug);
        }

        /** {@inheritDoc} */
        @Override protected Map<String, IgniteBiTuple<Integer, Integer>> run(Set<String> names) {
            Map<String, IgniteBiTuple<Integer, Integer>> total = new HashMap<>();
            ClusterNode locNode = g.localNode();

            for (IgniteInternalCache c : ignite.cachesx()) {
                String cacheName = c.name();
                Affinity<Object> aff = g.affinity(c.name());

                if (names.contains(cacheName)) {
                    Set<Cache.Entry> entries = c.entrySet();

                    int before = entries.size(), after = before;

                    for (Cache.Entry entry : entries) {
                        if (aff.isBackup(locNode, entry.getKey()) && c.evict(entry.getKey()))
                            after--;
                    }

                    total.put(cacheName, new IgniteBiTuple<>(before, after));
                }
            }

            return total;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorCachesSwapBackupsJob.class, this);
        }
    }
}