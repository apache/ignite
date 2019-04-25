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

package org.apache.ignite.internal.processors.hadoop.impl.v1;

import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskContext;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskInfo;
import org.apache.ignite.internal.processors.hadoop.impl.v2.HadoopV2TaskContext;

import java.io.IOException;

/**
 * Hadoop setup task implementation for v1 API.
 */
public class HadoopV1SetupTask extends HadoopV1Task {
    /**
     * Constructor.
     *
     * @param taskInfo Task info.
     */
    public HadoopV1SetupTask(HadoopTaskInfo taskInfo) {
        super(taskInfo);
    }

    /** {@inheritDoc} */
    @Override public void run(HadoopTaskContext taskCtx) throws IgniteCheckedException {
        HadoopV2TaskContext ctx = (HadoopV2TaskContext)taskCtx;

        try {
            ctx.jobConf().getOutputFormat().checkOutputSpecs(null, ctx.jobConf());

            OutputCommitter committer = ctx.jobConf().getOutputCommitter();

            if (committer != null)
                committer.setupJob(ctx.jobContext());
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
    }
}