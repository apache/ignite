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

package org.apache.ignite.internal.processors.hadoop.v1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.hadoop.HadoopFileBlock;
import org.apache.ignite.internal.processors.hadoop.HadoopInputSplit;
import org.apache.ignite.internal.processors.hadoop.HadoopUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Hadoop API v1 splitter.
 */
public class HadoopV1Splitter {
    /** */
    private static final String[] EMPTY_HOSTS = {};

    /**
     * @param jobConf Job configuration.
     * @return Collection of mapped splits.
     * @throws IgniteCheckedException If mapping failed.
     */
    public static Collection<HadoopInputSplit> splitJob(JobConf jobConf) throws IgniteCheckedException {
        try {
            InputFormat<?, ?> format = jobConf.getInputFormat();

            assert format != null;

            InputSplit[] splits = format.getSplits(jobConf, 0);

            Collection<HadoopInputSplit> res = new ArrayList<>(splits.length);

            for (int i = 0; i < splits.length; i++) {
                InputSplit nativeSplit = splits[i];

                if (nativeSplit instanceof FileSplit) {
                    FileSplit s = (FileSplit)nativeSplit;

                    res.add(new HadoopFileBlock(s.getLocations(), s.getPath().toUri(), s.getStart(), s.getLength()));
                }
                else
                    res.add(HadoopUtils.wrapSplit(i, nativeSplit, nativeSplit.getLocations()));
            }

            return res;
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * @param clsName Input split class name.
     * @param in Input stream.
     * @param hosts Optional hosts.
     * @return File block or {@code null} if it is not a {@link FileSplit} instance.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public static HadoopFileBlock readFileBlock(String clsName, FSDataInputStream in,
        @Nullable String[] hosts) throws IgniteCheckedException {
        if (!FileSplit.class.getName().equals(clsName))
            return null;

        FileSplit split = U.newInstance(FileSplit.class);

        try {
            split.readFields(in);
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }

        if (hosts == null)
            hosts = EMPTY_HOSTS;

        return new HadoopFileBlock(hosts, split.getPath().toUri(), split.getStart(), split.getLength());
    }
}