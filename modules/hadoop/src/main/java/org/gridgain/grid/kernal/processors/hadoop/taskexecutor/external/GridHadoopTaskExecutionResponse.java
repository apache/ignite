/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.taskexecutor.external;

import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.message.*;

import java.util.*;

/**
 * Job execution response.
 */
public class GridHadoopTaskExecutionResponse implements GridHadoopMessage {
    /** Job ID for which tasks has started. */
    private GridHadoopJobId jobId;

    /** Reducers assigned for external process. */
    private Collection<Integer> reducers;

    /**
     * @param jobId Job ID.
     * @param reducers Reducers for process.
     */
    public GridHadoopTaskExecutionResponse(GridHadoopJobId jobId, Collection<Integer> reducers) {
        this.jobId = jobId;
        this.reducers = reducers;
    }

    /**
     * @return Job ID.
     */
    public GridHadoopJobId jobId() {
        return jobId;
    }

    /**
     * @return Reducers.
     */
    public Collection<Integer> reducers() {
        return reducers;
    }
}
