/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.hadoop;

import java.io.*;

/**
 * The object that writes some system counters to some storage for each running job. This operation is a part of
 * whole statistics collection process.
 */
public interface GridHadoopStatWriter {
    /**
     * Writes counters of given job to some statistics storage.
     *
     * @param jobInfo Job info.
     * @param jobId Job id.
     * @param cntrs Counters.
     * @throws IOException If failed.
     */
    public void write(GridHadoopJobInfo jobInfo, GridHadoopJobId jobId, GridHadoopCounters cntrs) throws IOException;
}