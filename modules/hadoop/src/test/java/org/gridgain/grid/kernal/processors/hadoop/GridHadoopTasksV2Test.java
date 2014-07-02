/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.examples.*;
import org.gridgain.grid.kernal.processors.hadoop.v2.*;

import java.util.*;

/**
 * Tests of Map, Combine and Reduce task executions via running of job of hadoop API v2.
 */
public class GridHadoopTasksV2Test extends GridHadoopTasksAllVersionsTest {
    /**
     * Creates WordCount hadoop job for API v2.
     *
     * @param inFile Input file name for the job.
     * @param outFile Output file name for the job.
     * @return Hadoop job.
     * @throws Exception if fails.
     */
    @Override public GridHadoopJob getHadoopJob(String inFile, String outFile) throws Exception {
        Job job = Job.getInstance();

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        GridHadoopWordCount2.setTasksClasses(job, true, true, true);

        Configuration conf = job.getConfiguration();

        conf.set("fs.default.name", ggfsScheme());
        conf.set("fs.ggfs.impl", "org.gridgain.grid.ggfs.hadoop.v1.GridGgfsHadoopFileSystem");
        conf.set("fs.AbstractFileSystem.ggfs.impl", "org.gridgain.grid.ggfs.hadoop.v2.GridGgfsHadoopFileSystem");

        FileInputFormat.setInputPaths(job, new Path(inFile));
        FileOutputFormat.setOutputPath(job, new Path(outFile));

        job.setJarByClass(GridHadoopWordCount2.class);

        Job hadoopJob = GridHadoopWordCount2.getJob(inFile, outFile);

        GridHadoopDefaultJobInfo jobInfo = new GridHadoopDefaultJobInfo(hadoopJob.getConfiguration());

        GridHadoopJobId jobId = new GridHadoopJobId(new UUID(0, 0), 0);

        GridHadoopV2Job gridHadoopJob = new GridHadoopV2Job(jobId, jobInfo, log);

        hadoopJob.setJobID(gridHadoopJob.hadoopJobContext().getJobID());

        return gridHadoopJob;
    }

    /** {@inheritDoc} */
    @Override public String getOutputFileNamePrefix() {
        return "part-r-";
    }
}
