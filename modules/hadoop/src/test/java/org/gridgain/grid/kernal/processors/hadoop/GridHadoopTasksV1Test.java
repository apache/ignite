/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop;

import org.apache.hadoop.mapred.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.examples.*;
import org.gridgain.grid.kernal.processors.hadoop.v2.*;

import java.io.*;
import java.util.*;

import static org.gridgain.grid.kernal.processors.hadoop.GridHadoopUtils.*;

/**
 * Tests of Map, Combine and Reduce task executions via running of job of hadoop API v1.
 */
public class GridHadoopTasksV1Test extends GridHadoopTasksAllVersionsTest {
    /**
     * Creates WordCount hadoop job for API v1.
     *
     * @param inFile Input file name for the job.
     * @param outFile Output file name for the job.
     * @return Hadoop job.
     * @throws IOException If fails.
     */
    @Override public GridHadoopV2Job getHadoopJob(String inFile, String outFile) throws Exception {
        JobConf jobConf = GridHadoopWordCount1.getJob(inFile, outFile);

        setupFileSystems(jobConf);

        GridHadoopDefaultJobInfo jobInfo = createJobInfo(jobConf);

        GridHadoopJobId jobId = new GridHadoopJobId(new UUID(0, 0), 0);

        return new GridHadoopV2Job(jobId, jobInfo, log);
    }

    /** {@inheritDoc} */
    @Override public String getOutputFileNamePrefix() {
        return "part-";
    }
}
