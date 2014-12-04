/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.proto;

import org.apache.ignite.compute.*;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;

/**
 * Task to get the next job ID.
 */
public class GridHadoopProtocolNextTaskIdTask extends GridHadoopProtocolTaskAdapter<GridHadoopJobId> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override public GridHadoopJobId run(GridComputeJobContext jobCtx, GridHadoop hadoop,
        GridHadoopProtocolTaskArguments args) throws GridException {
        return hadoop.nextJobId();
    }
}
