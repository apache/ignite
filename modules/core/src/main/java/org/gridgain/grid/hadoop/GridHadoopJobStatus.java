/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.hadoop;

import org.gridgain.grid.*;

/**
 * Hadoop job status.
 */
public class GridHadoopJobStatus {
    /** Finish future. */
    private GridFuture<?> finishFut;

    /** Job info. */
    private GridHadoopJobInfo jobInfo;

    /**
     * @param finishFut Finish future.
     * @param jobInfo Job info.
     */
    public GridHadoopJobStatus(GridFuture<?> finishFut, GridHadoopJobInfo jobInfo) {
        this.finishFut = finishFut;
        this.jobInfo = jobInfo;
    }

    /**
     * Gets job execution finish future.
     *
     * @return Finish future.
     */
    public GridFuture<?> finishFuture() {
        return finishFut;
    }

    /**
     * Gets job info.
     *
     * @return Job info.
     */
    public GridHadoopJobInfo jobInfo() {
        return jobInfo;
    }
}
