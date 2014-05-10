/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.taskexecutor.external;

import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.jobtracker.*;
import org.gridgain.grid.kernal.processors.hadoop.message.*;

/**
 * Job info update request.
 */
public class GridHadoopJobInfoUpdateRequest extends GridHadoopMessage {
    /** Job ID. */
    private GridHadoopJobId jobId;

    /** Job phase. */
    private GridHadoopJobPhase jobPhase;

    /**
     * @param jobId Job ID.
     * @param jobPhase Job phase.
     */
    public GridHadoopJobInfoUpdateRequest(GridHadoopJobId jobId, GridHadoopJobPhase jobPhase) {
        this.jobId = jobId;
        this.jobPhase = jobPhase;
    }

    /**
     * @return Job ID.
     */
    public GridHadoopJobId jobId() {
        return jobId;
    }

    /**
     * @return Job phase.
     */
    public GridHadoopJobPhase jobPhase() {
        return jobPhase;
    }
}
