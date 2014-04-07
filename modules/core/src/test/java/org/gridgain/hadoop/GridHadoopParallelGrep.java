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
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.util.*;

import java.io.*;
import java.util.*;

/**
 *
 */
public class GridHadoopParallelGrep extends Configured implements Tool {

    private JobClient client;

    @Override public int run(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Grep <numJobs> <inDir> <outDir> <regex>");
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        JobConf jobConf = new JobConf(getConf());

        client = new JobClient(jobConf);

        int numJobs = Integer.parseInt(args[0]);

        Collection<RunningJob> jobs = new ArrayList<>(numJobs);

        for (int i = 0; i < numJobs; i++)
            jobs.add(submit(args[1], args[2]+i, args[3]));

        int ret = 0;

        for (RunningJob job: jobs) {
            if (!client.monitorAndPrintJob(jobConf, job)) {
                System.out.println("Job failed: " + job.getID());

                ret = -1;
            }
        }

        return ret;
    }

    private RunningJob submit(String input, String output, String regex) throws IOException {
        JobConf grepJob = new JobConf(getConf(), GridHadoopParallelGrep.class);

        grepJob.setJobName("grep-search");

        FileInputFormat.setInputPaths(grepJob, input);

        grepJob.setMapperClass(RegexMapper.class);
        grepJob.set("mapred.mapper.regex", regex);

        grepJob.setCombinerClass(LongSumReducer.class);
        grepJob.setReducerClass(LongSumReducer.class);

        FileOutputFormat.setOutputPath(grepJob, new Path(output));
        grepJob.setOutputFormat(SequenceFileOutputFormat.class);
        grepJob.setOutputKeyClass(Text.class);
        grepJob.setOutputValueClass(LongWritable.class);

        return client.submitJob(grepJob);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new GridHadoopParallelGrep(), args);
        System.exit(res);
    }
}
