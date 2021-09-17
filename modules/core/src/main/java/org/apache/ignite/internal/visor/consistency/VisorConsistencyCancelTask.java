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

package org.apache.ignite.internal.visor.consistency;

import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.systemview.view.ComputeJobView;

import static org.apache.ignite.internal.processors.job.GridJobProcessor.JOBS_VIEW;

/**
 * Cancels given consistency repairs on all cluster nodes.
 */
public class VisorConsistencyCancelTask extends VisorMultiNodeTask<Void, Void, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorConsistencyCancelJob job(Void arg) {
        return new VisorConsistencyCancelJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Override protected Void reduce0(List<ComputeJobResult> results) {
        // No-op, just awaiting all jobs done.
        return null;
    }

    /**
     * Job that cancels the tasks.
     */
    private static class VisorConsistencyCancelJob extends VisorJob<Void, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Auto-injected grid instance.
         */
        @IgniteInstanceResource
        private transient IgniteEx ignite;

        /**
         * Default constructor.
         */
        protected VisorConsistencyCancelJob(Void arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Void run(Void arg) throws IgniteException {
            F.iterator(ignite.context().systemView().view(JOBS_VIEW),
                ComputeJobView::sessionId,
                true,
                job -> job.taskClassName().equals(VisorConsistencyRepairTask.class.getName())
            ).forEach(sesId -> ignite.context().job().cancelJob(sesId, null, false));

            return null;
        }
    }
}
