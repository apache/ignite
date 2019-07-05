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

package org.apache.ignite.console.demo.service;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.ComputeTaskCancelledException;
import org.apache.ignite.console.demo.AgentDemoUtils;
import org.apache.ignite.console.demo.task.DemoCancellableTask;
import org.apache.ignite.console.demo.task.DemoComputeTask;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;

/**
 * Demo service. Run tasks on nodes. Run demo load on caches.
 */
public class DemoComputeLoadService implements Service {
    /** Ignite instance. */
    @IgniteInstanceResource
    private Ignite ignite;

    /** Thread pool to execute cache load operations. */
    private ScheduledExecutorService computePool;

    /** {@inheritDoc} */
    @Override public void cancel(ServiceContext ctx) {
        if (computePool != null)
            computePool.shutdownNow();
    }

    /** {@inheritDoc} */
    @Override public void init(ServiceContext ctx) {
        computePool = AgentDemoUtils.newScheduledThreadPool(2, "demo-compute-load-tasks");
    }

    /** {@inheritDoc} */
    @Override public void execute(ServiceContext ctx) {
        computePool.scheduleWithFixedDelay(() -> {
            try {
                ignite.compute().withNoFailover()
                    .execute(DemoComputeTask.class, null);
            }
            catch (ComputeTaskCancelledException ignore) {
                // No-op.
            }
            catch (Throwable e) {
                ignite.log().error("Task execution error", e);
            }
        }, 10, 3, TimeUnit.SECONDS);

        computePool.scheduleWithFixedDelay(() -> {
            try {
                ignite.compute().withNoFailover()
                    .execute(DemoCancellableTask.class, null);
            }
            catch (ComputeTaskCancelledException ignore) {
                // No-op.
            }
            catch (Throwable e) {
                ignite.log().error("DemoCancellableTask execution error", e);
            }
        }, 10, 30, TimeUnit.SECONDS);
    }
}
