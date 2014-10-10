/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.jobmetrics;

import org.gridgain.grid.kernal.processors.resource.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.*;
import org.gridgain.testframework.junits.common.*;

/**
 * Grid job metrics processor load test.
 */
public class GridJobMetricsProcessorLoadTest extends GridCommonAbstractTest {
    /** */
    private static final int THREADS_CNT = 10;

    /** */
    private GridTestKernalContext ctx;

    /** */
    public GridJobMetricsProcessorLoadTest() {
        super(/*start grid*/false);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ctx = newContext();

        ctx.add(new GridResourceProcessor(ctx));
        ctx.add(new GridJobMetricsProcessor(ctx));

        ctx.start();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        ctx.stop(true);
    }

    /**
     * @throws Exception if failed.
     */
    public void testJobMetricsMultiThreaded() throws Exception {
        GridTestUtils.runMultiThreaded(new Runnable() {
            @Override public void run() {
                try {
                    int i = 0;
                    while (i++ < 1000)
                        ctx.jobMetric().addSnapshot(new GridJobMetricsSnapshot());
                }
                catch (Exception e) {
                    fail(e.getMessage());
                }
            }
        }, THREADS_CNT, "grid-job-metrics-test");

        ctx.jobMetric().getJobMetrics();

        GridTestUtils.runMultiThreaded(new Runnable() {
            @Override public void run() {
                try {
                    int i = 0;
                    while (i++ < 100000)
                        ctx.jobMetric().addSnapshot(new GridJobMetricsSnapshot());
                }
                catch (Exception e) {
                    fail(e.getMessage());
                }
            }
        }, THREADS_CNT, "grid-job-metrics-test");

        ctx.jobMetric().getJobMetrics();
    }
}
