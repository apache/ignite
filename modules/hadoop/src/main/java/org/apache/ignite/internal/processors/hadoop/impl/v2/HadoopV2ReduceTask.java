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

package org.apache.ignite.internal.processors.hadoop.impl.v2;

import org.apache.hadoop.mapred.JobContextImpl;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.reduce.WrappedReducer;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.hadoop.HadoopMapperUtils;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskInfo;

/**
 * Hadoop reduce task implementation for v2 API.
 */
public class HadoopV2ReduceTask extends HadoopV2Task {
    /** {@code True} if reduce, {@code false} if combine. */
    private final boolean reduce;

    /**
     * Constructor.
     *
     * @param taskInfo Task info.
     * @param reduce {@code True} if reduce, {@code false} if combine.
     */
    public HadoopV2ReduceTask(HadoopTaskInfo taskInfo, boolean reduce) {
        super(taskInfo);

        this.reduce = reduce;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"ConstantConditions", "unchecked"})
    @Override public void run0(HadoopV2TaskContext taskCtx) throws IgniteCheckedException {
        OutputFormat outputFormat = null;
        Exception err = null;

        JobContextImpl jobCtx = taskCtx.jobContext();

        // Set mapper index for combiner tasks
        if (!reduce && taskCtx.taskInfo().hasMapperIndex())
            HadoopMapperUtils.mapperIndex(taskCtx.taskInfo().mapperIndex());
        else
            HadoopMapperUtils.clearMapperIndex();

        try {
            outputFormat = reduce || !taskCtx.job().info().hasReducer() ? prepareWriter(jobCtx) : null;

            Reducer reducer;

            if (reduce) reducer = ReflectionUtils.newInstance(jobCtx.getReducerClass(),
                jobCtx.getConfiguration());
            else reducer = ReflectionUtils.newInstance(jobCtx.getCombinerClass(),
                jobCtx.getConfiguration());

            try {
                reducer.run(new WrappedReducer().getReducerContext(hadoopContext()));

                if (!reduce)
                    taskCtx.onMapperFinished();
            }
            finally {
                closeWriter();
            }

            commit(outputFormat);
        }
        catch (InterruptedException e) {
            err = e;

            Thread.currentThread().interrupt();

            throw new IgniteInterruptedCheckedException(e);
        }
        catch (Exception e) {
            err = e;

            throw new IgniteCheckedException(e);
        }
        finally {
            if (!reduce)
                HadoopMapperUtils.clearMapperIndex();

            if (err != null)
                abort(outputFormat);
        }
    }
}