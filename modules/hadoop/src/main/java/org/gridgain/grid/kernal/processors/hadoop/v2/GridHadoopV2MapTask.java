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

package org.gridgain.grid.kernal.processors.hadoop.v2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobContextImpl;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.map.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.util.*;
import org.apache.ignite.*;
import org.apache.ignite.hadoop.*;

/**
 * Hadoop map task implementation for v2 API.
 */
public class GridHadoopV2MapTask extends GridHadoopV2Task {
    /**
     * @param taskInfo Task info.
     */
    public GridHadoopV2MapTask(GridHadoopTaskInfo taskInfo) {
        super(taskInfo);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"ConstantConditions", "unchecked"})
    @Override public void run0(GridHadoopV2TaskContext taskCtx) throws IgniteCheckedException {
        GridHadoopInputSplit split = info().inputSplit();

        InputSplit nativeSplit;

        if (split instanceof GridHadoopFileBlock) {
            GridHadoopFileBlock block = (GridHadoopFileBlock)split;

            nativeSplit = new FileSplit(new Path(block.file().toString()), block.start(), block.length(), null);
        }
        else
            nativeSplit = (InputSplit)taskCtx.getNativeSplit(split);

        assert nativeSplit != null;

        OutputFormat outputFormat = null;
        Exception err = null;

        JobContextImpl jobCtx = taskCtx.jobContext();

        try {
            InputFormat inFormat = ReflectionUtils.newInstance(jobCtx.getInputFormatClass(),
                hadoopContext().getConfiguration());

            RecordReader reader = inFormat.createRecordReader(nativeSplit, hadoopContext());

            reader.initialize(nativeSplit, hadoopContext());

            hadoopContext().reader(reader);

            GridHadoopJobInfo jobInfo = taskCtx.job().info();

            outputFormat = jobInfo.hasCombiner() || jobInfo.hasReducer() ? null : prepareWriter(jobCtx);

            Mapper mapper = ReflectionUtils.newInstance(jobCtx.getMapperClass(), hadoopContext().getConfiguration());

            try {
                mapper.run(new WrappedMapper().getMapContext(hadoopContext()));
            }
            finally {
                closeWriter();
            }

            commit(outputFormat);
        }
        catch (InterruptedException e) {
            err = e;

            Thread.currentThread().interrupt();

            throw new IgniteInterruptedException(e);
        }
        catch (Exception e) {
            err = e;

            throw new IgniteCheckedException(e);
        }
        finally {
            if (err != null)
                abort(outputFormat);
        }
    }
}
