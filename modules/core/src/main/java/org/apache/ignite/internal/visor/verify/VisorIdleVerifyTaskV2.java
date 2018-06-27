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

package org.apache.ignite.internal.visor.verify;

import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyResultV2;
import org.apache.ignite.internal.processors.cache.verify.VerifyBackupPartitionsTaskV2;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.resources.JobContextResource;

/**
 * Task to verify checksums of backup partitions.
 */
@GridInternal
public class VisorIdleVerifyTaskV2 extends VisorOneNodeTask<VisorIdleVerifyTaskArg, IdleVerifyResultV2> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<VisorIdleVerifyTaskArg, IdleVerifyResultV2> job(VisorIdleVerifyTaskArg arg) {
        return new VisorIdleVerifyJobV2(arg, debug);
    }

    /**
     *
     */
    private static class VisorIdleVerifyJobV2 extends VisorJob<VisorIdleVerifyTaskArg, IdleVerifyResultV2> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private ComputeTaskFuture<IdleVerifyResultV2> fut;

        /** Auto-inject job context. */
        @JobContextResource
        protected transient ComputeJobContext jobCtx;

        /**
         * @param arg Argument.
         * @param debug Debug.
         */
        private VisorIdleVerifyJobV2(VisorIdleVerifyTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected IdleVerifyResultV2 run(VisorIdleVerifyTaskArg arg) throws IgniteException {
            if (fut == null) {
                fut = ignite.compute().executeAsync(VerifyBackupPartitionsTaskV2.class, arg);

                if (!fut.isDone()) {
                    jobCtx.holdcc();

                    fut.listen(new IgniteInClosure<IgniteFuture<IdleVerifyResultV2>>() {
                        @Override public void apply(IgniteFuture<IdleVerifyResultV2> f) {
                            jobCtx.callcc();
                        }
                    });

                    return null;
                }
            }

            return fut.get();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorIdleVerifyJobV2.class, this);
        }
    }
}
