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
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.hadoop.HadoopJobInfo;
import org.apache.ignite.internal.processors.hadoop.HadoopMapperUtils;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskInfo;

/**
 * Hadoop map task implementation for v2 API.
 */
public class HadoopV2MapTask extends HadoopV2Task {
    /**
     * @param taskInfo Task info.
     */
    public HadoopV2MapTask(HadoopTaskInfo taskInfo) {
        super(taskInfo);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"ConstantConditions", "unchecked"})
    @Override public void run0(HadoopV2TaskContext taskCtx) throws IgniteCheckedException {
        OutputFormat outputFormat = null;
        Exception err = null;

        JobContextImpl jobCtx = taskCtx.jobContext();

        if (taskCtx.taskInfo().hasMapperIndex())
            HadoopMapperUtils.mapperIndex(taskCtx.taskInfo().mapperIndex());
        else
            HadoopMapperUtils.clearMapperIndex();

        try {
            HadoopV2Context hadoopCtx = hadoopContext();

            InputSplit nativeSplit = hadoopCtx.getInputSplit();

            if (nativeSplit == null)
                throw new IgniteCheckedException("Input split cannot be null.");

            InputFormat inFormat = ReflectionUtils.newInstance(jobCtx.getInputFormatClass(),
                hadoopCtx.getConfiguration());

            RecordReader reader = inFormat.createRecordReader(nativeSplit, hadoopCtx);

            reader.initialize(nativeSplit, hadoopCtx);

            hadoopCtx.reader(reader);

            HadoopJobInfo jobInfo = taskCtx.job().info();

            outputFormat = jobInfo.hasCombiner() || jobInfo.hasReducer() ? null : prepareWriter(jobCtx);

            Mapper mapper = ReflectionUtils.newInstance(jobCtx.getMapperClass(), hadoopCtx.getConfiguration());

            try {
                mapper.run(new WrappedMapper().getMapContext(hadoopCtx));

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
            HadoopMapperUtils.clearMapperIndex();

            if (err != null)
                abort(outputFormat);
        }
    }
}