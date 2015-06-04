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

package org.apache.ignite.internal.processors.hadoop;

import org.apache.hadoop.mapred.*;
import org.apache.ignite.internal.processors.hadoop.examples.*;

import java.io.*;
import java.util.*;

import static org.apache.ignite.internal.processors.hadoop.HadoopUtils.*;

/**
 * Tests of Map, Combine and Reduce task executions via running of job of hadoop API v1.
 */
public class HadoopTasksV1Test extends HadoopTasksAllVersionsTest {
    /**
     * Creates WordCount hadoop job for API v1.
     *
     * @param inFile Input file name for the job.
     * @param outFile Output file name for the job.
     * @return Hadoop job.
     * @throws IOException If fails.
     */
    @Override public HadoopJob getHadoopJob(String inFile, String outFile) throws Exception {
        JobConf jobConf = HadoopWordCount1.getJob(inFile, outFile);

        setupFileSystems(jobConf);

        HadoopDefaultJobInfo jobInfo = createJobInfo(jobConf);

        HadoopJobId jobId = new HadoopJobId(new UUID(0, 0), 0);

        return jobInfo.createJob(jobId, log);
    }

    /** {@inheritDoc} */
    @Override public String getOutputFileNamePrefix() {
        return "part-";
    }
}
