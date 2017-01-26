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

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.hadoop.HadoopJob;
import org.apache.ignite.internal.processors.hadoop.HadoopMapperUtils;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskCancelledException;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskContext;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskInfo;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskInput;
import org.apache.ignite.internal.processors.hadoop.impl.v2.HadoopV2TaskContext;

/**
 * Hadoop reduce task implementation for v1 API.
 */
public class HadoopV1ReduceTask extends HadoopV1Task {
    /** {@code True} if reduce, {@code false} if combine. */
    private final boolean reduce;

    /**
     * Constructor.
     *
     * @param taskInfo Task info.
     * @param reduce {@code True} if reduce, {@code false} if combine.
     */
    public HadoopV1ReduceTask(HadoopTaskInfo taskInfo, boolean reduce) {
        super(taskInfo);

        this.reduce = reduce;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void run(HadoopTaskContext taskCtx) throws IgniteCheckedException {
        HadoopJob job = taskCtx.job();

        HadoopV2TaskContext taskCtx0 = (HadoopV2TaskContext)taskCtx;

        if (!reduce && taskCtx.taskInfo().hasMapperIndex())
            HadoopMapperUtils.mapperIndex(taskCtx.taskInfo().mapperIndex());
        else
            HadoopMapperUtils.clearMapperIndex();

        try {
            JobConf jobConf = taskCtx0.jobConf();

            HadoopTaskInput input = taskCtx.input();

            HadoopV1OutputCollector collector = null;

            try {
                collector = collector(jobConf, taskCtx0, reduce || !job.info().hasReducer(), fileName(), taskCtx0.attemptId());

                Reducer reducer;
                if (reduce) reducer = ReflectionUtils.newInstance(jobConf.getReducerClass(),
                    jobConf);
                else reducer = ReflectionUtils.newInstance(jobConf.getCombinerClass(),
                    jobConf);

                assert reducer != null;

                try {
                    try {
                        while (input.next()) {
                            if (isCancelled())
                                throw new HadoopTaskCancelledException("Reduce task cancelled.");

                            reducer.reduce(input.key(), input.values(), collector, Reporter.NULL);
                        }

                        if (!reduce)
                            taskCtx.onMapperFinished();
                    }
                    finally {
                        reducer.close();
                    }
                }
                finally {
                    collector.closeWriter();
                }

                collector.commit();
            }
            catch (Exception e) {
                if (collector != null)
                    collector.abort();

                throw new IgniteCheckedException(e);
            }
        }
        finally {
            if (!reduce)
                HadoopMapperUtils.clearMapperIndex();
        }
    }
}