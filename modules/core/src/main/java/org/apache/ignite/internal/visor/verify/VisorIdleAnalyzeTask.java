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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.internal.processors.cache.verify.CollectConflictPartitionKeysTask;
import org.apache.ignite.internal.processors.cache.verify.PartitionEntryHashRecord;
import org.apache.ignite.internal.processors.cache.verify.PartitionHashRecord;
import org.apache.ignite.internal.processors.cache.verify.RetrieveConflictPartitionValuesTask;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.resources.JobContextResource;

/**
 * Task to find diverged keys of conflict partition.
 */
@GridInternal
public class VisorIdleAnalyzeTask extends VisorOneNodeTask<VisorIdleAnalyzeTaskArg, VisorIdleAnalyzeTaskResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<VisorIdleAnalyzeTaskArg, VisorIdleAnalyzeTaskResult> job(VisorIdleAnalyzeTaskArg arg) {
        return new VisorIdleVerifyJob(arg, debug);
    }

    /**
     *
     */
    private static class VisorIdleVerifyJob extends VisorJob<VisorIdleAnalyzeTaskArg, VisorIdleAnalyzeTaskResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private ComputeTaskFuture<Map<PartitionHashRecord, List<PartitionEntryHashRecord>>> conflictKeysFut;

        /** */
        private ComputeTaskFuture<Map<PartitionHashRecord, List<PartitionEntryHashRecord>>> conflictValsFut;

        /** Auto-inject job context. */
        @JobContextResource
        protected transient ComputeJobContext jobCtx;

        /**
         * @param arg Argument.
         * @param debug Debug.
         */
        private VisorIdleVerifyJob(VisorIdleAnalyzeTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected VisorIdleAnalyzeTaskResult run(VisorIdleAnalyzeTaskArg arg) throws IgniteException {
            if (conflictKeysFut == null) {
                conflictKeysFut = ignite.compute()
                    .executeAsync(CollectConflictPartitionKeysTask.class, arg.getPartitionKey());

                if (!conflictKeysFut.isDone()) {
                    jobCtx.holdcc();

                    conflictKeysFut.listen(new IgniteInClosure<IgniteFuture<Map<PartitionHashRecord, List<PartitionEntryHashRecord>>>>() {
                        @Override public void apply(IgniteFuture<Map<PartitionHashRecord, List<PartitionEntryHashRecord>>> f) {
                            jobCtx.callcc();
                        }
                    });

                    return null;
                }
            }

            Map<PartitionHashRecord, List<PartitionEntryHashRecord>> conflictKeys = conflictKeysFut.get();

            if (conflictKeys.isEmpty())
                return new VisorIdleAnalyzeTaskResult(Collections.emptyMap());

            if (conflictValsFut == null) {
                conflictValsFut = ignite.compute().executeAsync(RetrieveConflictPartitionValuesTask.class, conflictKeys);

                if (!conflictValsFut.isDone()) {
                    jobCtx.holdcc();

                    conflictKeysFut.listen(new IgniteInClosure<IgniteFuture<Map<PartitionHashRecord, List<PartitionEntryHashRecord>>>>() {
                        @Override public void apply(IgniteFuture<Map<PartitionHashRecord, List<PartitionEntryHashRecord>>> f) {
                            jobCtx.callcc();
                        }
                    });

                    return null;
                }
            }

            return new VisorIdleAnalyzeTaskResult(conflictValsFut.get());
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorIdleVerifyJob.class, this);
        }
    }
}
