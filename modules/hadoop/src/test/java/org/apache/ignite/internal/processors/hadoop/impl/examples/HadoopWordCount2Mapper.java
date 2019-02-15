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
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.ignite.internal.processors.hadoop.impl.HadoopErrorSimulator;

/**
 * Mapper phase of WordCount job.
 */
public class HadoopWordCount2Mapper extends Mapper<Object, Text, Text, IntWritable> implements Configurable {
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

        HadoopErrorSimulator.instance().onMap();
    }

    /** {@inheritDoc} */
    @Override protected void setup(Context ctx) throws IOException, InterruptedException {
        super.setup(ctx);

        wasSetUp = true;

        HadoopErrorSimulator.instance().onMapSetup();
    }

    /** {@inheritDoc} */
    @Override protected void cleanup(Context ctx) throws IOException, InterruptedException {
        super.cleanup(ctx);

        HadoopErrorSimulator.instance().onMapCleanup();
    }

    /** {@inheritDoc} */
    @Override public void setConf(Configuration conf) {
        wasConfigured = true;

        HadoopErrorSimulator.instance().onMapConfigure();
    }

    /** {@inheritDoc} */
    @Override public Configuration getConf() {
        return null;
    }
}