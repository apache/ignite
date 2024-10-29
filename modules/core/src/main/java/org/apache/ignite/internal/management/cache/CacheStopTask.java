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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.plugin.security.SecurityPermissionSet;

import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.NO_PERMISSIONS;

/**
 * Task that stop specified caches on specified node.
 */
@GridInternal
public class CacheStopTask extends VisorOneNodeTask<CacheDestroyCommandArg, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected CacheStopJob job(CacheDestroyCommandArg arg) {
        return new CacheStopJob(arg, debug);
    }

    /**
     * Job that stop specified caches.
     */
    private static class CacheStopJob extends VisorJob<CacheDestroyCommandArg, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job.
         *
         * @param arg Task argument.
         * @param debug Debug flag.
         */
        private CacheStopJob(CacheDestroyCommandArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Void run(CacheDestroyCommandArg arg) {
            Collection<String> cacheNames = F.isEmpty(arg.caches())
                ? F.asList(arg.caches())
                : new HashSet<>(Arrays.asList(arg.caches()));

            if (F.isEmpty(cacheNames))
                throw new IllegalStateException("Cache names was not specified.");

            ignite.destroyCaches(cacheNames);

            return null;
        }

        /** {@inheritDoc} */
        @Override public SecurityPermissionSet requiredPermissions() {
            // This task does nothing but delegates the call to the Ignite public API.
            // Therefore, it is safe to execute task without any additional permissions check.
            return NO_PERMISSIONS;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(CacheStopJob.class, this);
        }
    }
}
