/*
*  Copyright (C) GridGain Systems. All Rights Reserved.
*  _________        _____ __________________        _____
*  __  ____/___________(_)______  /__  ____/______ ____(_)_______
*  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
*  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
*  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
*/
package org.apache.ignite.internal.visor.verify;

import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.internal.processors.cache.verify.PartitionHashRecord;
import org.apache.ignite.internal.processors.cache.verify.PartitionKey;
import org.apache.ignite.internal.processors.cache.verify.VerifyBackupPartitionsTask;
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
    private static class VisorIdleVerifyJob extends VisorJob<VisorIdleVerifyTaskArg, VisorIdleVerifyTaskResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private ComputeTaskFuture<Map<PartitionKey, List<PartitionHashRecord>>> fut;

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
            if (fut == null) {
                fut = ignite.compute().executeAsync(VerifyBackupPartitionsTask.class, arg.getCaches());

                if (!fut.isDone()) {
                    jobCtx.holdcc();

                    fut.listen(new IgniteInClosure<IgniteFuture<Map<PartitionKey, List<PartitionHashRecord>>>>() {
                        @Override public void apply(IgniteFuture<Map<PartitionKey, List<PartitionHashRecord>>> f) {
                            jobCtx.callcc();
                        }
                    });

                    return null;
                }
            }

            return new VisorIdleVerifyTaskResult(fut.get());
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorIdleVerifyJob.class, this);
        }
    }
}
