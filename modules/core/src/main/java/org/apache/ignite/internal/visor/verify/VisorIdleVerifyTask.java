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

import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.verify.PartitionHashRecord;
import org.apache.ignite.internal.processors.cache.verify.PartitionKey;
import org.apache.ignite.internal.processors.cache.verify.VerifyBackupPartitionsTask;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.resources.JobContextResource;

import static org.apache.ignite.internal.processors.task.TaskExecutionOptions.options;

/**
 * Task to verify checksums of backup partitions.
 */
@GridInternal
@Deprecated
public class VisorIdleVerifyTask extends VisorOneNodeTask<VisorIdleVerifyTaskArg, VisorIdleVerifyTaskResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<VisorIdleVerifyTaskArg, VisorIdleVerifyTaskResult> job(VisorIdleVerifyTaskArg arg) {
        return new VisorIdleVerifyJob(arg, debug);
    }

    /**
     *
     */
    @Deprecated
    private static class VisorIdleVerifyJob extends VisorJob<VisorIdleVerifyTaskArg, VisorIdleVerifyTaskResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private IgniteInternalFuture<Map<PartitionKey, List<PartitionHashRecord>>> fut;

        /** Auto-inject job context. */
        @JobContextResource
        protected transient ComputeJobContext jobCtx;

        /**
         * @param arg Argument.
         * @param debug Debug.
         */
        private VisorIdleVerifyJob(VisorIdleVerifyTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected VisorIdleVerifyTaskResult run(VisorIdleVerifyTaskArg arg) throws IgniteException {
            try {
                if (fut == null) {
                    fut = ignite.context().task().execute(
                        VerifyBackupPartitionsTask.class,
                        arg.caches(),
                        options(ignite.cluster().forServers().nodes())
                    );

                    if (!fut.isDone()) {
                        jobCtx.holdcc();

                        fut.listen(new IgniteInClosure<IgniteInternalFuture<Map<PartitionKey, List<PartitionHashRecord>>>>() {
                            @Override public void apply(IgniteInternalFuture<Map<PartitionKey, List<PartitionHashRecord>>> f) {
                                jobCtx.callcc();
                            }
                        });

                        return null;
                    }
                }

                return new VisorIdleVerifyTaskResult(fut.get());
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorIdleVerifyJob.class, this);
        }
    }
}
