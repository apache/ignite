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

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.ComputeMXBeanImpl;
import org.apache.ignite.internal.management.api.NoArg;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.systemview.view.ComputeTaskView;
import org.apache.ignite.spi.systemview.view.SystemView;

import static org.apache.ignite.internal.processors.task.GridTaskProcessor.TASKS_VIEW;

/**
 * Task that cancels idle_verify command.
 */
public class CacheIdleVerifyCancelTask extends VisorOneNodeTask<NoArg, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final String IDLE_VERIFY = "idle_verify";

    /** {@inheritDoc} */
    @Override protected VisorJob<NoArg, Void> job(NoArg arg) {
        return new CacheIdleVerifyCancelJob(idleVerifyId(), debug);
    }

    /**
     * @return Retrieves idle_verify command session id.
     */
    private IgniteUuid idleVerifyId() {
        SystemView<ComputeTaskView> views = ignite.context().systemView().view(TASKS_VIEW);

        int idleVerifyCnt = 0;

        ComputeTaskView foundView = null;

        for (ComputeTaskView view : views) {
            if (view.taskName().equals(IDLE_VERIFY)) {
                idleVerifyCnt++;

                foundView = view;
            }
        }

        if (idleVerifyCnt == 0)
            throw new IgniteException("Failed to find idle verify task to cancel.");

        if (idleVerifyCnt > 1)
            throw new IgniteException("Idle verify tasks can only be executed once.");

        assert foundView != null : "Failed to find idle verify task to cancel.";

        return foundView.id();
    }

    /**
     * Job that cancels idle_verify command.
     */
    private class CacheIdleVerifyCancelJob extends VisorJob<NoArg, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final IgniteUuid idleVerifyId;

        /**
         * @param idleVerifyId Session id of idle_verify command.
         * @param debug Debug flag.
         */
        public CacheIdleVerifyCancelJob(IgniteUuid idleVerifyId, boolean debug) {
            super(new NoArg(), debug);
            this.idleVerifyId = idleVerifyId;
        }

        /** {@inheritDoc} */
        @Override protected Void run(NoArg arg) {
            new ComputeMXBeanImpl(ignite.context()).cancel(idleVerifyId);

            return null;
        }
    }
}
