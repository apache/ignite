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

package org.gridgain.grid.kernal.processors.hadoop;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.examples.*;
import org.gridgain.grid.kernal.processors.hadoop.v2.*;

import java.util.*;

import static org.gridgain.grid.kernal.processors.hadoop.GridHadoopUtils.*;

/**
 * Tests of Map, Combine and Reduce task executions via running of job of hadoop API v2.
 */
public class GridHadoopTasksV2Test extends GridHadoopTasksAllVersionsTest {
    /**
     * Creates WordCount hadoop job for API v2.
     *
     * @param inFile Input file name for the job.
     * @param outFile Output file name for the job.
     * @return Hadoop job.
     * @throws Exception if fails.
     */
    @Override public GridHadoopV2Job getHadoopJob(String inFile, String outFile) throws Exception {
        Job job = Job.getInstance();

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        GridHadoopWordCount2.setTasksClasses(job, true, true, true);

        Configuration conf = job.getConfiguration();

        setupFileSystems(conf);

        FileInputFormat.setInputPaths(job, new Path(inFile));
        FileOutputFormat.setOutputPath(job, new Path(outFile));

        job.setJarByClass(GridHadoopWordCount2.class);

        Job hadoopJob = GridHadoopWordCount2.getJob(inFile, outFile);

        GridHadoopDefaultJobInfo jobInfo = createJobInfo(hadoopJob.getConfiguration());

        GridHadoopJobId jobId = new GridHadoopJobId(new UUID(0, 0), 0);

        return new GridHadoopV2Job(jobId, jobInfo, log);
    }

    /** {@inheritDoc} */
    @Override public String getOutputFileNamePrefix() {
        return "part-r-";
    }
}
