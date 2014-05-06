/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.jobtracker;

import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.v2.*;

/**
 * Default Hadoop job factory.
 */
public class GridHadoopDefaultJobFactory implements GridHadoopJobFactory {
    /** {@inheritDoc} */
    @Override public GridHadoopJob createJob(GridHadoopJobId id, GridHadoopJobInfo jobInfo) {
        if (jobInfo instanceof GridHadoopDefaultJobInfo) {
            GridHadoopDefaultJobInfo info = (GridHadoopDefaultJobInfo)jobInfo;

            return new GridHadoopV2Job(id, info);
        }
        else
            throw new GridRuntimeException("Unsupported job info class: " + jobInfo.getClass().getName());
    }
}
