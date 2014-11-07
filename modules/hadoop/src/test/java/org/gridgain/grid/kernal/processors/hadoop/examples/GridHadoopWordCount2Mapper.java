/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.examples;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

/**
 * Mapper phase of WordCount job.
 */
public class GridHadoopWordCount2Mapper extends Mapper<Object, Text, Text, IntWritable> implements Configurable {
    /** Writable container for writing word. */
    private Text word = new Text();

    /** Writable integer constant of '1' is writing as count of found words. */
    private static final IntWritable one = new IntWritable(1);

    /** Flag is to check that mapper was configured before run. */
    private boolean wasConfigured;

    /** Flag is to check that mapper was set up before run. */
    private boolean wasSetUp;

    /** {@inheritDoc} */
    @Override public void map(Object key, Text val, Context ctx) throws IOException, InterruptedException {
        assert wasConfigured : "Mapper should be configured";
        assert wasSetUp : "Mapper should be set up";

        StringTokenizer wordList = new StringTokenizer(val.toString());

        while (wordList.hasMoreTokens()) {
            word.set(wordList.nextToken());

            ctx.write(word, one);
        }
    }

    /** {@inheritDoc} */
    @Override protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        wasSetUp = true;
    }

    /** {@inheritDoc} */
    @Override public void setConf(Configuration conf) {
        wasConfigured = true;
    }

    /** {@inheritDoc} */
    @Override public Configuration getConf() {
        return null;
    }
}
