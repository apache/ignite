/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.management.cache;

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;

import static org.apache.ignite.internal.management.cache.ViewCacheCmd.CACHES;
import static org.apache.ignite.internal.management.cache.ViewCacheCmd.GROUPS;
import static org.apache.ignite.internal.management.cache.ViewCacheCmd.SEQ;

/**
 *
 */
@GridInternal
public class ViewCacheTask extends VisorOneNodeTask<CacheListCommandArg, ViewCacheTaskResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<CacheListCommandArg, ViewCacheTaskResult> job(CacheListCommandArg arg) {
        return new VisorViewCacheJob(arg, debug);
    }

    /**
     *
     */
    private static class VisorViewCacheJob extends VisorJob<CacheListCommandArg, ViewCacheTaskResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Argument.
         * @param debug Debug.
         */
        protected VisorViewCacheJob(CacheListCommandArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected ViewCacheTaskResult run(CacheListCommandArg arg) throws IgniteException {
            try {
                ViewCacheCmd cmd = arg.groups()
                    ? GROUPS
                    : (arg.seq() ? SEQ : CACHES);

                ViewCacheClosure clo = new ViewCacheClosure(arg.regex(), cmd);

                ignite.context().resource().injectGeneric(clo);

                return new ViewCacheTaskResult(clo.call());
            }
            catch (Exception e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorViewCacheJob.class, this);
        }
    }
}
