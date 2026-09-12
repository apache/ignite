

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
