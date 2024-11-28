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

import java.util.Optional;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.ComputeMXBeanImpl;
import org.apache.ignite.internal.management.api.NoArg;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.systemview.view.ComputeTaskView;

import static org.apache.ignite.internal.processors.task.GridTaskProcessor.TASKS_VIEW;

/**
 * Task that cancels idle_verify command.
 */
public class CacheIdleVerifyCancelTask extends VisorOneNodeTask<NoArg, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<NoArg, Void> job(NoArg arg) {
        return new CacheIdleVerifyCancelJob(debug);
    }

    /**
     * Job that cancels idle_verify command.
     */
    private class CacheIdleVerifyCancelJob extends VisorJob<NoArg, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param debug Debug flag.
         */
        public CacheIdleVerifyCancelJob(boolean debug) {
            super(new NoArg(), debug);
        }

        /** {@inheritDoc} */
        @Override protected Void run(NoArg arg) {
            if (idleVerifyId().isPresent())
                new ComputeMXBeanImpl(ignite.context()).cancel(idleVerifyId().get());

            return null;
        }

        /**
         * @return Retrieves idle_verify command session id if present.
         */
        private Optional<IgniteUuid> idleVerifyId() {
            int idleVerifyCnt = 0;

            ComputeTaskView foundView = null;

            for (ComputeTaskView view : ignite.context().systemView().<ComputeTaskView>view(TASKS_VIEW)) {
                if (view.taskName().equals(IdleVerifyTaskV2.class.getName())) {
                    idleVerifyCnt++;

                    foundView = view;
                }
            }

            switch (idleVerifyCnt) {
                case 0:
                    return Optional.empty();
                case 1:
                    return Optional.of(foundView.id());
                default:
                    throw new IgniteException("Invalid running idle verify command count: " + idleVerifyCnt);
            }
        }
    }
}
