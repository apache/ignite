/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.hadoop;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import java.io.*;
import java.util.*;

/**
 *
 */
public class GridHadoopParallelCount extends Configured implements Tool {
    /** Job client. */
    private JobClient client;

    /** {@inheritDoc} */
    @Override public int run(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("<numJobs> <inDir> <outDir>");

            ToolRunner.printGenericCommandUsage(System.out);

            return -1;
        }

        JobConf jobConf = new JobConf(getConf());

        client = new JobClient(jobConf);

        int numJobs = Integer.parseInt(args[0]);

        System.out.println("Starting jobs: " + numJobs);

        Collection<RunningJob> jobs = new ArrayList<>(numJobs);

        for (int i = 0; i < numJobs; i++)
            jobs.add(submit(args[1], args[2] + i));

        int ret = 0;

        for (RunningJob job: jobs) {
            if (!client.monitorAndPrintJob(jobConf, job)) {
                System.out.println("Job failed: " + job.getID());

                ret = -1;
            }
        }

        return ret;
    }

    /**
     * Submits a job to hadoop.
     *
     * @param in Input file.
     * @param out Output file.
     * @return The job.
     * @throws IOException If failed.
     */
    private RunningJob submit(String in, String out) throws IOException {
        JobConf jobConf = new JobConf(getConf(), GridHadoopParallelCount.class);

        jobConf.setJobName("count-chars");

        FileInputFormat.setInputPaths(jobConf, in);

        jobConf.setMapperClass(GridHadoopTextLineMapper.class);

        jobConf.setCombinerClass(GridHadoopTextLineReducer.class);
        jobConf.setReducerClass(GridHadoopTextLineReducer.class);

        FileOutputFormat.setOutputPath(jobConf, new Path(out));

        jobConf.setOutputFormat(TextOutputFormat.class);
        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(LongWritable.class);

        return client.submitJob(jobConf);
    }

    /**
     * Main method.
     *
     * @param args Parameters.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new GridHadoopParallelCount(), args);

        System.exit(res);
    }
}
