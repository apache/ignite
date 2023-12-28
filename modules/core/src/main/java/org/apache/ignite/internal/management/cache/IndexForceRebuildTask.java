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

package org.apache.ignite.internal.management.cache;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;

/**
 * Task that triggers indexes force rebuild for specified caches or cache groups.
 */
@GridInternal
public class IndexForceRebuildTask extends VisorMultiNodeTask<CacheIndexesForceRebuildCommandArg,
    Map<UUID, IndexForceRebuildTaskRes>, IndexForceRebuildTaskRes> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected IndexForceRebuildJob job(CacheIndexesForceRebuildCommandArg arg) {
        return new IndexForceRebuildJob(arg, debug);
    }

    /** */
    private static class IndexForceRebuildJob extends VisorJob<CacheIndexesForceRebuildCommandArg, IndexForceRebuildTaskRes> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected IndexForceRebuildJob(CacheIndexesForceRebuildCommandArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected IndexForceRebuildTaskRes run(CacheIndexesForceRebuildCommandArg arg) throws IgniteException {
            assert (arg.cacheNames() == null) ^ (arg.groupNames() == null) :
                "Either cacheNames or cacheGroups must be specified.";

            Set<GridCacheContext> cachesToRebuild = new HashSet<>();
            Set<String> notFound = new HashSet<>();

            final GridCacheProcessor cacheProc = ignite.context().cache();

            if (arg.cacheNames() != null) {
                for (String cacheName : arg.cacheNames()) {
                    IgniteInternalCache cache = cacheProc.cache(cacheName);

                    if (cache != null)
                        cachesToRebuild.add(cache.context());
                    else
                        notFound.add(cacheName);
                }
            }
            else {
                for (String cacheGrpName : arg.groupNames()) {
                    CacheGroupContext grpCtx = cacheProc.cacheGroup(CU.cacheId(cacheGrpName));

                    if (grpCtx != null)
                        cachesToRebuild.addAll(grpCtx.caches());
                    else
                        notFound.add(cacheGrpName);
                }
            }

            Collection<GridCacheContext> cachesCtxWithRebuildingInProgress =
                ignite.context().cache().context().database().forceRebuildIndexes(cachesToRebuild);

            Set<IndexRebuildStatusInfoContainer> cachesWithRebuildingInProgress =
                cachesCtxWithRebuildingInProgress.stream()
                    .map(IndexRebuildStatusInfoContainer::new)
                    .collect(Collectors.toSet());

            Set<IndexRebuildStatusInfoContainer> cachesWithStartedRebuild =
                cachesToRebuild.stream()
                    .map(IndexRebuildStatusInfoContainer::new)
                    .filter(c -> !cachesWithRebuildingInProgress.contains(c))
                    .collect(Collectors.toSet());

            return new IndexForceRebuildTaskRes(
                cachesWithStartedRebuild,
                cachesWithRebuildingInProgress,
                notFound
            );
        }
    }

    /** {@inheritDoc} */
    @Override protected Map<UUID, IndexForceRebuildTaskRes> reduce0(List<ComputeJobResult> results)
        throws IgniteException {

        Map<UUID, IndexForceRebuildTaskRes> res = new HashMap<>();

        for (ComputeJobResult jobRes : results) {
            if (jobRes.getException() != null)
                throw jobRes.getException();

            res.put(jobRes.getNode().id(), jobRes.getData());
        }

        return res;
    }
}
