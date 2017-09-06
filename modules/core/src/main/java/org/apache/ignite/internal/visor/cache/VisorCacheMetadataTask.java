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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlMetadata;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.escapeName;

/**
 * Task to get cache SQL metadata.
 */
@GridInternal
public class VisorCacheMetadataTask extends VisorOneNodeTask<VisorCacheMetadataTaskArg, VisorCacheSqlMetadata> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorCacheMetadataJob job(VisorCacheMetadataTaskArg arg) {
        return new VisorCacheMetadataJob(arg, debug);
    }

    /**
     * Job to get cache SQL metadata.
     */
    private static class VisorCacheMetadataJob extends VisorJob<VisorCacheMetadataTaskArg, VisorCacheSqlMetadata> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Cache name to take metadata.
         * @param debug Debug flag.
         */
        private VisorCacheMetadataJob(VisorCacheMetadataTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected VisorCacheSqlMetadata run(VisorCacheMetadataTaskArg arg) {
            try {
                IgniteInternalCache<Object, Object> cache = ignite.cachex(arg.getCacheName());

                if (cache != null) {
                    GridCacheSqlMetadata meta = F.first(cache.context().queries().sqlMetadata());

                    if (meta != null)
                        return new VisorCacheSqlMetadata(meta);

                    return null;
                }

                throw new IgniteException("Cache not found: " + escapeName(arg.getCacheName()));
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorCacheMetadataJob.class, this);
        }
    }
}
