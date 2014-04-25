/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.examples;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/**
 * Example job for testing hadoop task execution.
 */
public class GridGainWordCount1 {
    /**
     * Entry point to start job.
     * @param args command line parameters.
     * @throws Exception if fails.
     */
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("usage: [input] [output]");
            System.exit(-1);
        }

        JobConf job = getJob(args[0], args[1]);

        JobClient.runJob(job);
    }

    /**
     * Gets fully configured JobConf instance.
     *
     * @param input input file name.
     * @param output output directory name.
     * @return Job configuration
     */
    public static JobConf getJob(String input, String output) {
        JobConf conf = new JobConf(GridGainWordCount1.class);
        conf.setJobName("wordcount");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(GridGainWordCount1Map.class);
        conf.setCombinerClass(GridGainWordCount1Reduce.class);
        conf.setReducerClass(GridGainWordCount1Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));

        return conf;
    }


}
