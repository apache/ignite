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

package org.apache.ignite.internal.processors.hadoop.impl.v1;

import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskContext;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskInfo;
import org.apache.ignite.internal.processors.hadoop.impl.v2.HadoopV2TaskContext;

import java.io.IOException;

/**
 * Hadoop cleanup task implementation for v1 API.
 */
public class HadoopV1CleanupTask extends HadoopV1Task {
    /** Abort flag. */
    private final boolean abort;

    /**
     * @param taskInfo Task info.
     * @param abort Abort flag.
     */
    public HadoopV1CleanupTask(HadoopTaskInfo taskInfo, boolean abort) {
        super(taskInfo);

        this.abort = abort;
    }

    /** {@inheritDoc} */
    @Override public void run(HadoopTaskContext taskCtx) throws IgniteCheckedException {
        HadoopV2TaskContext ctx = (HadoopV2TaskContext)taskCtx;

        JobContext jobCtx = ctx.jobContext();

        try {
            OutputCommitter committer = jobCtx.getJobConf().getOutputCommitter();

            if (abort)
                committer.abortJob(jobCtx, JobStatus.State.FAILED);
            else
                committer.commitJob(jobCtx);
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
    }
}