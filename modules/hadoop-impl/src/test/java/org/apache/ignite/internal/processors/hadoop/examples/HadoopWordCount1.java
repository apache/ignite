/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.hadoop.examples;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 * Example job for testing hadoop task execution.
 */
public class HadoopWordCount1 {
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
        JobConf conf = new JobConf(HadoopWordCount1.class);
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
            jobConf.setMapperClass(HadoopWordCount1Map.class);
            jobConf.setInputFormat(TextInputFormat.class);
        }

        if (setCombiner)
            jobConf.setCombinerClass(HadoopWordCount1Reduce.class);

        if (setReducer) {
            jobConf.setReducerClass(HadoopWordCount1Reduce.class);
            jobConf.setOutputFormat(TextOutputFormat.class);
        }
    }
}