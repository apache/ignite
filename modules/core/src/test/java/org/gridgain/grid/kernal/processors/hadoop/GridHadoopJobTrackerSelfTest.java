/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop;

import org.apache.hadoop.conf.*;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.hadoop.hadoop2impl.*;

import java.net.*;
import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * Job tracker self test.
 */
public class GridHadoopJobTrackerSelfTest extends GridHadoopAbstractSelfTest {
    /** Test block count parameter name. */
    private static final String BLOCK_CNT = "test.block.count";

    /** Task execution count. */
    private static final AtomicInteger execCnt = new AtomicInteger();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(gridCount());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override public GridHadoopConfiguration hadoopConfiguration(String gridName) {
        GridHadoopConfiguration cfg = super.hadoopConfiguration(gridName);

        cfg.setJobFactory(new HadoopTestJobFactory());
        cfg.setMapReducePlanner(new GridHadoopTestRoundRobinMrPlanner());

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testSimpleTaskSubmit() throws Exception {
        GridKernal kernal = (GridKernal)grid(0);

        GridHadoopProcessor hadoop = kernal.context().hadoop();

        UUID globalId = UUID.randomUUID();

        Configuration cfg = new Configuration();

        int cnt = 10;

        cfg.setInt(BLOCK_CNT, cnt);

        GridFuture<?> execFut = hadoop.submit(new GridHadoopJobId(globalId, 1), new GridHadoopDefaultJobInfo(cfg));

        execFut.get();

        assertEquals(cnt, execCnt.get());
    }

    /**
     * Test job factory.
     */
    private static class HadoopTestJobFactory implements GridHadoopJobFactory {
        /** {@inheritDoc} */
        @Override public GridHadoopJob createJob(GridHadoopJobId id, GridHadoopJobInfo jobInfo) {
            return new HadoopTestJob(id, (GridHadoopDefaultJobInfo)jobInfo);
        }
    }

    /**
     * Test job.
     */
    private static class HadoopTestJob extends GridHadoopV2JobImpl {
        /**
         * @param jobId Job ID.
         * @param jobInfoImpl Job info.
         */
        private HadoopTestJob(GridHadoopJobId jobId, GridHadoopDefaultJobInfo jobInfoImpl) {
            super(jobId, jobInfoImpl);
        }

        /** {@inheritDoc} */
        @Override public Collection<GridHadoopFileBlock> input() throws GridException {
            int blocks = jobInfo.configuration().getInt(BLOCK_CNT, 0);

            Collection<GridHadoopFileBlock> res = new ArrayList<>(blocks);

            try {
                for (int i = 0; i < blocks; i++)
                    res.add(new GridHadoopFileBlock(new String[] {"localhost"}, new URI("someFile"), i, i + 1));

                return res;
            }
            catch (URISyntaxException e) {
                throw new GridException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public GridHadoopTask createTask(GridHadoopTaskInfo taskInfo) {
            return new HadoopTestTask(taskInfo);
        }
    }

    /**
     * Test task.
     */
    private static class HadoopTestTask extends GridHadoopV2TaskImpl {
        /**
         * @param taskInfo Task info.
         */
        private HadoopTestTask(GridHadoopTaskInfo taskInfo) {
            super(taskInfo);
        }

        /** {@inheritDoc} */
        @Override public void run(GridHadoopTaskContext ctx) {
            execCnt.incrementAndGet();
        }
    }
}
