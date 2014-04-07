/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.hadoop;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.io.*;
import java.util.*;

/**
 * Reducer and combiner.
 */
public class GridHadoopTextLineReducer extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable> {
    /** {@inheritDoc} */
    @Override public void reduce(Text key, Iterator<LongWritable> vals, OutputCollector<Text, LongWritable> output,
        Reporter reporter) throws IOException {
        // Sum all values for this key.
        long sum = 0;

        while (vals.hasNext())
            sum += vals.next().get();

        // Output sum.
        output.collect(key, new LongWritable(sum));
    }
}
