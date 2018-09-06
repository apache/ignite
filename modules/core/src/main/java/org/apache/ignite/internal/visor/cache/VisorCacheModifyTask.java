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

import java.util.UUID;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.internal.visor.query.VisorQueryUtils;
import org.apache.ignite.internal.visor.util.VisorTaskUtils;

/**
 * Task that modify value in specified cache.
 */
@GridInternal
public class VisorCacheModifyTask extends VisorOneNodeTask<VisorCacheModifyTaskArg, VisorCacheModifyTaskResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorCacheModifyJob job(VisorCacheModifyTaskArg arg) {
        return new VisorCacheModifyJob(arg, debug);
    }

    /**
     * Job that modify value in specified cache.
     */
    private static class VisorCacheModifyJob extends VisorJob<VisorCacheModifyTaskArg, VisorCacheModifyTaskResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job.
         *
         * @param arg Task argument.
         * @param debug Debug flag.
         */
        private VisorCacheModifyJob(VisorCacheModifyTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected VisorCacheModifyTaskResult run(final VisorCacheModifyTaskArg arg) {
            assert arg != null;

            VisorModifyCacheMode mode = arg.getMode();
            String cacheName = arg.getCacheName();
            Object key = arg.getKey();

            assert mode != null;
            assert cacheName != null;
            assert key != null;

            IgniteCache<Object, Object> cache = ignite.cache(cacheName);

            if (cache == null)
                throw new IllegalArgumentException("Failed to find cache with specified name [cacheName=" + arg.getCacheName() + "]");

            ClusterNode node = ignite.affinity(cacheName).mapKeyToNode(key);

            UUID nid = node != null ? node.id() : null;

            switch (mode) {
                case PUT:
                    Object old = cache.get(key);

                    cache.put(key, arg.getValue());

                    return new VisorCacheModifyTaskResult(nid, VisorTaskUtils.compactClass(old),
                        VisorQueryUtils.convertValue(old));

                case GET:
                    Object val = cache.get(key);

                    return new VisorCacheModifyTaskResult(nid, VisorTaskUtils.compactClass(val),
                        VisorQueryUtils.convertValue(val));

                case REMOVE:
                    Object rmv = cache.get(key);

                    cache.remove(key);

                    return new VisorCacheModifyTaskResult(nid, VisorTaskUtils.compactClass(rmv),
                        VisorQueryUtils.convertValue(rmv));
            }

            return new VisorCacheModifyTaskResult(nid, null, null);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorCacheModifyJob.class, this);
        }
    }
}
