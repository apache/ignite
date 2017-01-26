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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.hadoop.HadoopFileBlock;
import org.apache.ignite.internal.processors.hadoop.HadoopInputSplit;
import org.apache.ignite.internal.processors.hadoop.HadoopJob;
import org.apache.ignite.internal.processors.hadoop.HadoopMapperUtils;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskCancelledException;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskContext;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskInfo;
import org.apache.ignite.internal.processors.hadoop.impl.v2.HadoopV2TaskContext;

/**
 * Hadoop map task implementation for v1 API.
 */
public class HadoopV1MapTask extends HadoopV1Task {
    /** */
    private static final String[] EMPTY_HOSTS = new String[0];

    /**
     * Constructor.
     *
     * @param taskInfo Taks info.
     */
    public HadoopV1MapTask(HadoopTaskInfo taskInfo) {
        super(taskInfo);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void run(HadoopTaskContext taskCtx) throws IgniteCheckedException {
        HadoopJob job = taskCtx.job();

        HadoopV2TaskContext taskCtx0 = (HadoopV2TaskContext)taskCtx;

        if (taskCtx.taskInfo().hasMapperIndex())
            HadoopMapperUtils.mapperIndex(taskCtx.taskInfo().mapperIndex());
        else
            HadoopMapperUtils.clearMapperIndex();

        try {
            JobConf jobConf = taskCtx0.jobConf();

            InputFormat inFormat = jobConf.getInputFormat();

            HadoopInputSplit split = info().inputSplit();

            InputSplit nativeSplit;

            if (split instanceof HadoopFileBlock) {
                HadoopFileBlock block = (HadoopFileBlock)split;

                nativeSplit = new FileSplit(new Path(block.file().toString()), block.start(), block.length(), EMPTY_HOSTS);
            }
            else
                nativeSplit = (InputSplit)taskCtx0.getNativeSplit(split);

            assert nativeSplit != null;

            Reporter reporter = new HadoopV1Reporter(taskCtx);

            HadoopV1OutputCollector collector = null;

            try {
                collector = collector(jobConf, taskCtx0, !job.info().hasCombiner() && !job.info().hasReducer(),
                    fileName(), taskCtx0.attemptId());

                RecordReader reader = inFormat.getRecordReader(nativeSplit, jobConf, reporter);

                Mapper mapper = ReflectionUtils.newInstance(jobConf.getMapperClass(), jobConf);

                Object key = reader.createKey();
                Object val = reader.createValue();

                assert mapper != null;

                try {
                    try {
                        while (reader.next(key, val)) {
                            if (isCancelled())
                                throw new HadoopTaskCancelledException("Map task cancelled.");

                            mapper.map(key, val, collector, reporter);
                        }

                        taskCtx.onMapperFinished();
                    }
                    finally {
                        mapper.close();
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
            HadoopMapperUtils.clearMapperIndex();
        }
    }
}