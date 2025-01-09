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

import java.util.List;

import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.management.api.NoArg;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.systemview.view.ComputeJobView;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.job.GridJobProcessor.JOBS_VIEW;

/**
 * Task that cancels idle_verify command.
 */
public class CacheIdleVerifyCancelTask extends VisorMultiNodeTask<NoArg, Void, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<NoArg, Void> job(NoArg arg) {
        return new CacheIdleVerifyCancelJob(debug);
    }

    /** {@inheritDoc} */
    @Override protected @Nullable Void reduce0(List list) throws IgniteException {
        return null;
    }

    /**
     * Job that cancels idle_verify command.
     */
    private class CacheIdleVerifyCancelJob extends VisorJob<NoArg, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Injected logger. */
        @LoggerResource
        private IgniteLogger log;

        /**
         * Auto-injected grid instance.
         */
        @IgniteInstanceResource
        private transient IgniteEx ignite;

        /**
         * @param debug Debug flag.
         */
        public CacheIdleVerifyCancelJob(boolean debug) {
            super(new NoArg(), debug);
        }

        /** {@inheritDoc} */
        @Override protected Void run(NoArg arg) {
            cancelJob(IdleVerifyTaskV2.class);

            cancelJob(VerifyBackupPartitionsTaskV2.class);

            return null;
        }

        /**
         * @param taskCls Job class.
         */
        private void cancelJob(Class<?> taskCls) {
            F.iterator(
                ignite.context().systemView().view(JOBS_VIEW),
                ComputeJobView::sessionId,
                true,
                jobView -> jobView.taskClassName().equals(taskCls.getName())
            ).forEach(sesId -> ignite.context().job().cancelJob(sesId, null, false));
        }
    }
}
