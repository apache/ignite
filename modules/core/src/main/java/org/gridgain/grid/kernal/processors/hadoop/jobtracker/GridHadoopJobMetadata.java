/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.jobtracker;

import org.gridgain.grid.hadoop.*;

import java.util.*;

/**
 * Hadoop job metadata. Internal object used for distributed job state tracking.
 */
public class GridHadoopJobMetadata {
    /** Job ID. */
    private GridHadoopJobId jobId;

    /** Mapping mappers to nodes. */
    private UUID[] mappers;

    /** Mapping reducers to nodes. */
    private UUID reducers;

    /**
     * @param jobId Job ID.
     */
    public GridHadoopJobMetadata(GridHadoopJobId jobId) {
        this.jobId = jobId;
    }
}
