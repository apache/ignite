/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.hadoop.impl.examples;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.ignite.internal.processors.hadoop.impl.HadoopErrorSimulator;

/**
 * Combiner and Reducer phase of WordCount job.
 */
public class HadoopWordCount1Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
    /** Flag is to check that mapper was configured before run. */
    private boolean wasConfigured;

    /** {@inheritDoc} */
    @Override public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException {
        assert wasConfigured : "Reducer should be configured";

        int sum = 0;

        while (values.hasNext())
            sum += values.next().get();

        output.collect(key, new IntWritable(sum));

        HadoopErrorSimulator.instance().onReduce();
    }

    /** {@inheritDoc} */
    @Override public void configure(JobConf job) {
        super.configure(job);

        wasConfigured = true;

        HadoopErrorSimulator.instance().onReduceConfigure();
    }
}