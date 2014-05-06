/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.proto;

import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.*;

import java.util.*;

/**
 * Job status task.
 */
public class GridHadoopProtocolJobStatusTask extends GridHadoopProtocolTaskAdapter<GridHadoopJobStatus> {
    /** {@inheritDoc} */
    @Override public GridHadoopJobStatus run(GridHadoopProcessorAdapter proc, GridHadoopProtocolTaskArguments args)
        throws GridException {
        UUID nodeId = UUID.fromString(args.<String>get(0));
        int id = args.get(1);
        Long pollDelay = args.get(2);

        GridHadoopJobId jobId = new GridHadoopJobId(nodeId, id);

        if (pollDelay == null)
            pollDelay = proc.config().getJobStatusPollDelay();

        if (pollDelay > 0) {
            GridFuture<?> fut = proc.finishFuture(jobId);

            if (fut == null)
                return null;
            else {
                try {
                    fut.get(pollDelay);
                }
                catch (GridException ignore) {
                    // No-op.
                }
            }
        }

        return proc.status(jobId);
    }
}
