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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Example job for testing hadoop task execution.
 */
public class HadoopWordCount2 {
    /**
     * Entry point to start job.
     *
     * @param args Command line parameters.
     * @throws Exception If fails.
     */
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("usage: [input] [output]");
            System.exit(-1);
        }

        Job job = getJob(args[0], args[1]);

        job.submit();
    }

    /**
     * Gets fully configured Job instance.
     *
     * @param input Input file name.
     * @param output Output directory name.
     * @return Job instance.
     * @throws IOException If fails.
     */
    public static Job getJob(String input, String output) throws IOException {
        Job job = Job.getInstance();

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        setTasksClasses(job, true, true, true, false);

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setJarByClass(HadoopWordCount2.class);

        return job;
    }

    /**
     * Sets task classes with related info if needed into configuration object.
     *
     * @param job Configuration to change.
     * @param setMapper Option to set mapper and input format classes.
     * @param setCombiner Option to set combiner class.
     * @param setReducer Option to set reducer and output format classes.
     */
    public static void setTasksClasses(Job job, boolean setMapper, boolean setCombiner, boolean setReducer,
            boolean outputCompression) {
        if (setMapper) {
            job.setMapperClass(HadoopWordCount2Mapper.class);
            job.setInputFormatClass(TextInputFormat.class);
        }

        if (setCombiner)
            job.setCombinerClass(HadoopWordCount2Combiner.class);

        if (setReducer) {
            job.setReducerClass(HadoopWordCount2Reducer.class);
            job.setOutputFormatClass(TextOutputFormat.class);
        }

        if (outputCompression) {
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

            SequenceFileOutputFormat.setCompressOutput(job, true);

            job.getConfiguration().set(FileOutputFormat.COMPRESS_CODEC, SnappyCodec.class.getName());
        }
    }
}