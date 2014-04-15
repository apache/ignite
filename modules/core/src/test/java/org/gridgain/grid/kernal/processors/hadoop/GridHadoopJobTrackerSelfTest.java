/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop;

import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.*;

import java.util.*;

/**
 * Job tracker self test.
 */
public class GridHadoopJobTrackerSelfTest extends GridHadoopAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(gridCount());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testSimpleTaskSubmit() throws Exception {
        GridKernal kernal = (GridKernal)grid(0);

        GridHadoopProcessor hadoop = kernal.context().hadoop();

        UUID globalId = UUID.randomUUID();

        GridFuture<?> execFut = hadoop.submit(new GridHadoopJobId(globalId, 1), new HadoopJobTestInfo());
    }

    private static class HadoopJobTestInfo implements GridHadoopJobInfo {

    }

    private static class HadoopTestJobFactory implements GridHadoopJobFactory {
        @Override public GridHadoopJob createJob(GridHadoopJobId id, GridHadoopJobInfo jobInfo) {
            // TODO: implement.
            return null;
        }
    }
}
