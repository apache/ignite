/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.hadoop.impl.examples;

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
import org.apache.ignite.internal.processors.hadoop.impl.HadoopErrorSimulator;

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

        HadoopErrorSimulator.instance().onMap();
    }

    /** {@inheritDoc} */
    @Override public void configure(JobConf job) {
        super.configure(job);

        wasConfigured = true;

        HadoopErrorSimulator.instance().onMapConfigure();
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        super.close();

        HadoopErrorSimulator.instance().onMapClose();
    }
}