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
import org.apache.ignite.IgniteCache;
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

            // Collect info about indexes being rebuilt.
            Set<IndexRebuildStatusInfoContainer> rebuildIdxCaches =
                ignite.context().cache().publicCaches()
                    .stream()
                    .filter(c -> !c.indexReadyFuture().isDone())
                    .map(this::fromIgniteCache)
                    .collect(Collectors.toSet());

            if (arg.cacheNames() == null && arg.cacheGrps() == null) {
                assert false : "Neither cache names nor cache groups specified";

                return null;
            }

            Set<String> rebuildIdxCachesNames = rebuildIdxCaches.stream()
                .map(IndexRebuildStatusInfoContainer::cacheName)
                .collect(Collectors.toSet());

            Set<GridCacheContext> cachesToRebuild;
            Set<String> notFoundCache;

            final GridCacheProcessor cacheProcessor = ignite.context().cache();

            if (arg.cacheNames() != null) {
                notFoundCache = arg.cacheNames().stream()
                    .filter(name -> cacheProcessor.cache(name) == null)
                    .collect(Collectors.toSet());

                cachesToRebuild = cacheProcessor.publicCaches()
                    .stream()
                    .filter(c -> !rebuildIdxCachesNames.contains(c.getName()))
                    .filter(c -> arg.cacheNames().contains(c.getName()))
                    .map(IgniteCacheProxy::context)
                    .collect(Collectors.toSet());
            }
            else { //arg.cacheGrps() != null
                notFoundCache = arg.cacheGrps().stream()
                    .filter(grpName -> cacheProcessor.cacheGroup(CU.cacheId(grpName)) == null)
                    .collect(Collectors.toSet());

                cachesToRebuild = cacheProcessor.cacheGroups()
                    .stream()
                    .filter(grpContext -> arg.cacheGrps().contains(grpContext.name()))
                    .map(CacheGroupContext::caches)
                    .flatMap(List::stream)
                    .filter(c -> !rebuildIdxCachesNames.contains(c.name()))
                    .collect(Collectors.toSet());
            }

            ignite.context().cache().context().database().forceRebuildIndexes(cachesToRebuild);

            Set<IndexRebuildStatusInfoContainer> cachesWithStartedRebuild =
                cachesToRebuild.stream()
                    .map(c -> new IndexRebuildStatusInfoContainer(c.config()))
                    .collect(Collectors.toSet());

            return new IndexForceRebuildTaskRes(cachesWithStartedRebuild, rebuildIdxCaches, notFoundCache);

        }

        /** */
        private IndexRebuildStatusInfoContainer fromIgniteCache(IgniteCache c) {
            return new IndexRebuildStatusInfoContainer((CacheConfiguration)c.getConfiguration(CacheConfiguration.class));
        }
    }
}
