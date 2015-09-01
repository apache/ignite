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

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * Mapper phase of WordCount job.
 */
public class HadoopWordCount1Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
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