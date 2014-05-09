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
    @Override public GridHadoopJob getHadoopJob(String inFile, String outFile) throws Exception {
        JobConf hadoopJob = GridHadoopWordCount1.getJob(inFile, outFile);

        GridHadoopDefaultJobInfo jobInfo = new GridHadoopDefaultJobInfo(hadoopJob);

        GridHadoopJobId jobId = new GridHadoopJobId(new UUID(0, 0), 0);

        GridHadoopV2Job gridHadoopJob = new GridHadoopV2Job(jobId, jobInfo);

        return gridHadoopJob;
    }

    /** {@inheritDoc} */
    @Override public String getOutputFileNamePrefix() {
        return "part-";
    }

    /** {@inheritDoc} */
    @Override public void testMapTask() throws Exception {
        super.testMapTask();
    }

    /** {@inheritDoc} */
    @Override public void testReduceTask() throws Exception {
        super.testReduceTask();
    }

    /** {@inheritDoc} */
    @Override public void testCombinerTask() throws Exception {
        super.testCombinerTask();
    }

    /** {@inheritDoc} */
    @Override public void testAllTasks() throws Exception {
        super.testAllTasks();
    }
}
