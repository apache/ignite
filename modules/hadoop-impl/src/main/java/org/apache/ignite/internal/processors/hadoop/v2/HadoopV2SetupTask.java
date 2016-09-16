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

package org.apache.ignite.internal.processors.hadoop.v2;

import java.io.IOException;
import org.apache.hadoop.mapred.JobContextImpl;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskInfo;

/**
 * Hadoop setup task (prepares job).
 */
public class HadoopV2SetupTask extends HadoopV2Task {
    /**
     * Constructor.
     *
     * @param taskInfo task info.
     */
    public HadoopV2SetupTask(HadoopTaskInfo taskInfo) {
        super(taskInfo);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ConstantConditions")
    @Override protected void run0(HadoopV2TaskContext taskCtx) throws IgniteCheckedException {
        try {
            JobContextImpl jobCtx = taskCtx.jobContext();

            OutputFormat outputFormat = getOutputFormat(jobCtx);

            outputFormat.checkOutputSpecs(jobCtx);

            OutputCommitter committer = outputFormat.getOutputCommitter(hadoopContext());

            if (committer != null)
                committer.setupJob(jobCtx);
        }
        catch (ClassNotFoundException | IOException e) {
            throw new IgniteCheckedException(e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedCheckedException(e);
        }
    }
}