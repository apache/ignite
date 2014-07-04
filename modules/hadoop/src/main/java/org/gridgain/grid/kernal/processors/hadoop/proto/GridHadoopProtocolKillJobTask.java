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

/**
 * Kill job task.
 */
public class GridHadoopProtocolKillJobTask extends GridHadoopProtocolTaskAdapter<Boolean> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override public Boolean run(GridComputeJobContext jobCtx, GridHadoop hadoop,
        GridHadoopProtocolTaskArguments args) throws GridException {
        UUID nodeId = UUID.fromString(args.<String>get(0));
        Integer id = args.get(1);

        assert nodeId != null;
        assert id != null;

        GridHadoopJobId jobId = new GridHadoopJobId(nodeId, id);

        return hadoop.kill(jobId);
    }
}
