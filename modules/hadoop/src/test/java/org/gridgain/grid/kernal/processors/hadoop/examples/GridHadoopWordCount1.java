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
public class GridHadoopWordCount1 {
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
        JobConf conf = new JobConf(GridHadoopWordCount1.class);
        conf.setJobName("wordcount");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        setTasksClasses(conf, true, true, true);

        FileInputFormat.setInputPaths(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));

        return conf;
    }

    /**
     * Sets task classes with related info if needed into configuration object.
     *
     * @param jobConf Configuration to change.
     * @param setMapper Option to set mapper and input format classes.
     * @param setCombiner Option to set combiner class.
     * @param setReducer Option to set reducer and output format classes.
     */
    public static void setTasksClasses(JobConf jobConf, boolean setMapper, boolean setCombiner, boolean setReducer) {
        if (setMapper) {
            jobConf.setMapperClass(GridHadoopWordCount1Map.class);
            jobConf.setInputFormat(TextInputFormat.class);
        }

        if (setCombiner)
            jobConf.setCombinerClass(GridHadoopWordCount1Reduce.class);

        if (setReducer) {
            jobConf.setReducerClass(GridHadoopWordCount1Reduce.class);
            jobConf.setOutputFormat(TextOutputFormat.class);
        }
    }
}
