/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.shuffle.collections;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.v2.*;
import org.gridgain.testframework.junits.common.*;

import java.io.IOException;
import java.util.*;

/**
 * Abstract class for maps test.
 */
public abstract class GridHadoopAbstractMapTest extends GridCommonAbstractTest {
    public GridHadoopJob mockJob() throws GridException, IOException {
        Job jobCtx = Job.getInstance();

        jobCtx.setMapOutputKeyClass(IntWritable.class);
        jobCtx.setMapOutputValueClass(IntWritable.class);

        GridHadoopDefaultJobInfo jobInfo = new GridHadoopDefaultJobInfo(jobCtx.getConfiguration());

        return new GridHadoopV2Job(new GridHadoopJobId(UUID.randomUUID(), 10), jobInfo, log);
    }

    public GridHadoopTaskContext mockTaskContext(GridHadoopJob job) throws GridException {
        return job.getTaskContext(new GridHadoopTaskInfo(null, GridHadoopTaskType.MAP, null, 0, 0, null));
    }
}
