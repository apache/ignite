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

package org.apache.ignite.internal.visor.cache.index;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Task that triggers indexes force rebuild for specified caches or cache groups.
 */
@GridInternal
public class IndexForceRebuildTask extends VisorOneNodeTask<IndexForceRebuildTaskArg, IndexForceRebuildTaskRes> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected IndexForceRebuildJob job(IndexForceRebuildTaskArg arg) {
        return new IndexForceRebuildJob(arg, debug);
    }

    /** */
    private static class IndexForceRebuildJob extends VisorJob<IndexForceRebuildTaskArg, IndexForceRebuildTaskRes> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected IndexForceRebuildJob(@Nullable IndexForceRebuildTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected IndexForceRebuildTaskRes run(@Nullable IndexForceRebuildTaskArg arg)
            throws IgniteException
        {
            //Either cacheNames or cacheGrps must be specified
            assert (arg.cacheNames() == null) ^ (arg.cacheGrps() == null) :
                "Either cacheNames or cacheGroups must be specified";

            Set<IndexRebuildStatusInfoContainer> rebuildIdxCaches = getCachesWithIndicesBeingRebuilt();

            if (arg.cacheNames() == null && arg.cacheGrps() == null) {
                assert false : "Neither cache names nor cache groups specified";

                return null;
            }

            Set<String> cacheNamesToExclude = rebuildIdxCaches.stream()
                .map(IndexRebuildStatusInfoContainer::cacheName)
                .collect(Collectors.toSet());

            Set<GridCacheContext> cachesToRebuild;
            Set<String> notFoundCache;

            final GridCacheProcessor cacheProcessor = ignite.context().cache();

            if (arg.cacheNames() != null) {
                notFoundCache = arg.cacheNames().stream()
                    .filter(name -> cacheProcessor.cache(name) == null)
                    .collect(Collectors.toSet());

                cachesToRebuild = getCachesToRebuildByName(arg.cacheNames(), cacheNamesToExclude);
            }
            else { //arg.cacheGrps() != null
                notFoundCache = arg.cacheGrps().stream()
                    .filter(grpName -> cacheProcessor.cacheGroup(CU.cacheId(grpName)) == null)
                    .collect(Collectors.toSet());

                cachesToRebuild = getCachesToRebuildByGrp(arg.cacheGrps(), cacheNamesToExclude);
            }

            ignite.context().cache().context().database().forceRebuildIndexes(cachesToRebuild);

            Set<IndexRebuildStatusInfoContainer> cachesWithStartedRebuild =
                cachesToRebuild.stream()
                    .map(c -> new IndexRebuildStatusInfoContainer(c.config()))
                    .collect(Collectors.toSet());

            return new IndexForceRebuildTaskRes(cachesWithStartedRebuild, rebuildIdxCaches, notFoundCache);
        }

        /**
         * Caches with indices being rebuilt.
         *
         * @return Set of caches info with indices being rebuilt.
         */
        private Set<IndexRebuildStatusInfoContainer> getCachesWithIndicesBeingRebuilt() {
            return ignite.context().cache().publicCaches()
                .stream()
                .filter(cache -> !cache.indexReadyFuture().isDone())
                .map(cache -> cache.getConfiguration(CacheConfiguration.class))
                .map(IndexRebuildStatusInfoContainer::new)
                .collect(Collectors.toSet());
        }

        /**
         * Collects cache contexts of caches that can be rebuilt atm.
         *
         * @param cacheNames Whitelist of cacheNames.
         * @param cacheNamesToExclude Blacklist of cacheNames.
         * @return Set of cache context that can be rebuilt.
         */
        private Set<GridCacheContext> getCachesToRebuildByName(Set<String> cacheNames, Set<String> cacheNamesToExclude) {
            return ignite.context().cache().publicCaches()
                .stream()
                .filter(c -> !cacheNamesToExclude.contains(c.getName()))
                .filter(c -> cacheNames.contains(c.getName()))
                .map(IgniteCacheProxy::context)
                .collect(Collectors.toSet());
        }

        /**
         * Collects cache contexts of caches that can be rebuilt atm.
         *
         * @param cacheGrps Whitelist of cacheGroups.
         * @param cacheNamesToExclude Blacklist of cacheNames.
         * @return Set of cache context that can be rebuilt.
         */
        private Set<GridCacheContext> getCachesToRebuildByGrp(Set<String> cacheGrps, Set<String> cacheNamesToExclude) {
            return ignite.context().cache().cacheGroups()
                .stream()
                .filter(grpContext -> cacheGrps.contains(grpContext.name()))
                .map(CacheGroupContext::caches)
                .flatMap(List::stream)
                .filter(c -> !cacheNamesToExclude.contains(c.name()))
                .collect(Collectors.toSet());
        }
    }
}
