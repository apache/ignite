/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
import org.apache.ignite.hadoop.HadoopInputSplit;
import org.apache.ignite.internal.processors.hadoop.HadoopFileBlock;
import org.apache.ignite.internal.processors.hadoop.HadoopJobEx;
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
        HadoopJobEx job = taskCtx.job();

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

            Reporter reporter = new HadoopV1Reporter(taskCtx, nativeSplit);

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