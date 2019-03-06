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

package org.apache.ignite.internal.processors.hadoop.impl;

import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.ignite.internal.processors.hadoop.HadoopDefaultJobInfo;
import org.apache.ignite.internal.processors.hadoop.HadoopJobEx;
import org.apache.ignite.internal.processors.hadoop.HadoopJobId;
import org.apache.ignite.internal.processors.hadoop.HadoopHelperImpl;
import org.apache.ignite.internal.processors.hadoop.impl.examples.HadoopWordCount2;
import org.apache.ignite.internal.processors.hadoop.impl.v2.HadoopV2Job;

import static org.apache.ignite.internal.processors.hadoop.impl.HadoopUtils.createJobInfo;

/**
 * Tests of Map, Combine and Reduce task executions via running of job of hadoop API v2.
 */
public class HadoopTasksV2Test extends HadoopTasksVersionsAbstractTest {
    /**
     * Creates WordCount hadoop job for API v2.
     *
     * @param inFile Input file name for the job.
     * @param outFile Output file name for the job.
     * @return Hadoop job.
     * @throws Exception if fails.
     */
    @Override public HadoopJobEx getHadoopJob(String inFile, String outFile) throws Exception {
        Job job = Job.getInstance();

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        HadoopWordCount2.setTasksClasses(job, true, true, true, false);

        Configuration conf = job.getConfiguration();

        setupFileSystems(conf);

        FileInputFormat.setInputPaths(job, new Path(inFile));
        FileOutputFormat.setOutputPath(job, new Path(outFile));

        job.setJarByClass(HadoopWordCount2.class);

        Job hadoopJob = HadoopWordCount2.getJob(inFile, outFile);

        HadoopDefaultJobInfo jobInfo = createJobInfo(hadoopJob.getConfiguration(), null);

        UUID uuid = new UUID(0, 0);

        HadoopJobId jobId = new HadoopJobId(uuid, 0);

        return jobInfo.createJob(HadoopV2Job.class, jobId, log, null, new HadoopHelperImpl());
    }

    /** {@inheritDoc} */
    @Override public String getOutputFileNamePrefix() {
        return "part-r-";
    }
}
