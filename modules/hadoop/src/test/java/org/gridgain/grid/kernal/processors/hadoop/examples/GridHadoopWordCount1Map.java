/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.examples;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.io.*;
import java.util.*;

/**
 * Mapper phase of WordCount job.
 */
public class GridHadoopWordCount1Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    /** Writable integer constant of '1' is writing as count of found words. */
    private static final IntWritable one = new IntWritable(1);

    /** Writable container for writing word. */
    private Text word = new Text();

    /** Flag is to check that mapper was configured before run. */
    private boolean wasConfigured;

    /** {@inheritDoc} */
    @Override public void map(LongWritable key, Text val, OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException {

        assert wasConfigured : "Mapper should be configured";

        String line = val.toString();

        StringTokenizer tokenizer = new StringTokenizer(line);

        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());

            output.collect(word, one);
        }
    }

    /** {@inheritDoc} */
    @Override public void configure(JobConf job) {
        super.configure(job);

        wasConfigured = true;
    }
}
