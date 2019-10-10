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

package org.apache.ignite.console.demo.service;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
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
    @Override public void init(ServiceContext ctx) throws Exception {
        computePool = AgentDemoUtils.newScheduledThreadPool(2, "demo-compute-load-tasks");
    }

    /** {@inheritDoc} */
    @Override public void execute(ServiceContext ctx) throws Exception {
        computePool.scheduleWithFixedDelay(new Runnable() {
            @Override public void run() {
                try {
                    ignite.compute().withNoFailover()
                        .execute(DemoComputeTask.class, null);
                }
                catch (Throwable e) {
                    ignite.log().error("Task execution error", e);
                }
            }
        }, 10, 3, TimeUnit.SECONDS);

        computePool.scheduleWithFixedDelay(new Runnable() {
            @Override public void run() {
                try {
                    ignite.compute().withNoFailover()
                        .execute(DemoCancellableTask.class, null);
                }
                catch (Throwable e) {
                    ignite.log().error("DemoCancellableTask execution error", e);
                }
            }
        }, 10, 30, TimeUnit.SECONDS);
    }
}
