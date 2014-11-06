/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.proto;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.hadoop.*;

import java.util.*;

import static org.gridgain.grid.hadoop.GridHadoopJobPhase.*;

/**
 * Submit job task.
 */
public class GridHadoopProtocolSubmitJobTask extends GridHadoopProtocolTaskAdapter<GridHadoopJobStatus> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override public GridHadoopJobStatus run(GridComputeJobContext jobCtx, GridHadoop hadoop,
        GridHadoopProtocolTaskArguments args) throws GridException {
        UUID nodeId = UUID.fromString(args.<String>get(0));
        Integer id = args.get(1);
        GridHadoopDefaultJobInfo info = args.get(2);

        assert nodeId != null;
        assert id != null;
        assert info != null;

        GridHadoopJobId jobId = new GridHadoopJobId(nodeId, id);

        hadoop.submit(jobId, info);

        GridHadoopJobStatus res = hadoop.status(jobId);

        if (res == null) { // Submission failed.
            res = new GridHadoopJobStatus(jobId, info.jobName(), info.user(), 0, 0, 0, 0,
                PHASE_CANCELLING, true, 1);
        }

        return res;
    }
}
