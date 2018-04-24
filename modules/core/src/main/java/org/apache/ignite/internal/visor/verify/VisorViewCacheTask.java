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
package org.apache.ignite.internal.visor.verify;

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.commandline.cache.CacheCommand;
import org.apache.ignite.internal.processors.cache.verify.ViewCacheClosure;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
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
            String newSeqVal = arg.command() == CacheCommand.UPDATE_SEQ ? arg.newUpdateSequenceValue() : null;

            try {
                return new VisorViewCacheTaskResult(new ViewCacheClosure(arg.regex(), arg.command(), newSeqVal).call());
            }
            catch (Exception e) {
                throw new IgniteException(e);
            }
        }
    }
}
