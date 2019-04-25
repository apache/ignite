/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.visor.verify;

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.verify.ViewCacheClosure;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
@GridInternal
public class VisorViewCacheTask extends VisorOneNodeTask<VisorViewCacheTaskArg, VisorViewCacheTaskResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<VisorViewCacheTaskArg, VisorViewCacheTaskResult> job(VisorViewCacheTaskArg arg) {
        return new VisorViewCacheJob(arg, debug);
    }

    /**
     *
     */
    private static class VisorViewCacheJob extends VisorJob<VisorViewCacheTaskArg, VisorViewCacheTaskResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Argument.
         * @param debug Debug.
         */
        protected VisorViewCacheJob(@Nullable VisorViewCacheTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected VisorViewCacheTaskResult run(@Nullable VisorViewCacheTaskArg arg) throws IgniteException {
            try {
                ViewCacheClosure clo = new ViewCacheClosure(arg.regex(), arg.command());

                ignite.context().resource().injectGeneric(clo);

                return new VisorViewCacheTaskResult(clo.call());
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
