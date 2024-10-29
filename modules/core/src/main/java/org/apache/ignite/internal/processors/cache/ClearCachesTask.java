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

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.management.cache.CacheClearCommandArg;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.NO_PERMISSIONS;

/** Clears specified caches. */
@GridInternal
public class ClearCachesTask extends VisorOneNodeTask<CacheClearCommandArg, ClearCachesTaskResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @Override protected VisorJob<CacheClearCommandArg, ClearCachesTaskResult> job(CacheClearCommandArg arg) {
        return new ClearCacheJob(arg, debug);
    }

    /** Job clears specified caches. */
    private static class ClearCacheJob extends VisorJob<CacheClearCommandArg, ClearCachesTaskResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Local Ignite instance. */
        private Ignite ignite;

        /** */
        private ClearCacheJob(CacheClearCommandArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected ClearCachesTaskResult run(@Nullable CacheClearCommandArg arg) throws IgniteException {
            List<String> clearedCaches = new ArrayList<>();
            List<String> nonExistentCaches = new ArrayList<>();

            for (String cache: arg.caches()) {
                IgniteCache<?, ?> ignCache = ignite.cache(cache);

                if (ignCache == null)
                    nonExistentCaches.add(cache);
                else {
                    ignCache.clear();

                    clearedCaches.add(cache);
                }
            }

            return new ClearCachesTaskResult(clearedCaches, nonExistentCaches);
        }

        /** {@inheritDoc} */
        @Override public SecurityPermissionSet requiredPermissions() {
            // This task does nothing but delegates the call to the Ignite public API.
            // Therefore, it is safe to execute task without any additional permissions check.
            return NO_PERMISSIONS;
        }

        /** */
        @IgniteInstanceResource
        public void setIgnite(Ignite ignite) {
            this.ignite = ignite;
        }
    }
}
