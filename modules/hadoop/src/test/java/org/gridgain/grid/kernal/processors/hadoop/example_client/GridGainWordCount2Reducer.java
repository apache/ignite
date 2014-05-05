/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.example_client;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.*;

/**
 * Combiner and Reducer phase of WordCount job.
 */
public class GridGainWordCount2Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    /** Writable container for writing sum of word counts. */
    private IntWritable totalWordCnt = new IntWritable();

    /** {@inheritDoc} */
    @Override public void reduce(Text key, Iterable<IntWritable> values, Context ctx) throws IOException, InterruptedException {
        int wordCnt = 0;

        for (IntWritable value : values) {
            wordCnt += value.get();
        }

        totalWordCnt.set(wordCnt);
        ctx.write(key, totalWordCnt);
    }
}
