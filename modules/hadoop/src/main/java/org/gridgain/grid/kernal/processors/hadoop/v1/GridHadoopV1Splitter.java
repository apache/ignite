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

package org.gridgain.grid.kernal.processors.hadoop.v1;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.*;
import org.apache.ignite.*;
import org.apache.ignite.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Hadoop API v1 splitter.
 */
public class GridHadoopV1Splitter {
    /** */
    private static final String[] EMPTY_HOSTS = {};

    /**
     * @param jobConf Job configuration.
     * @return Collection of mapped splits.
     * @throws IgniteCheckedException If mapping failed.
     */
    public static Collection<GridHadoopInputSplit> splitJob(JobConf jobConf) throws IgniteCheckedException {
        try {
            InputFormat<?, ?> format = jobConf.getInputFormat();

            assert format != null;

            InputSplit[] splits = format.getSplits(jobConf, 0);

            Collection<GridHadoopInputSplit> res = new ArrayList<>(splits.length);

            for (int i = 0; i < splits.length; i++) {
                InputSplit nativeSplit = splits[i];

                if (nativeSplit instanceof FileSplit) {
                    FileSplit s = (FileSplit)nativeSplit;

                    res.add(new GridHadoopFileBlock(s.getLocations(), s.getPath().toUri(), s.getStart(), s.getLength()));
                }
                else
                    res.add(GridHadoopUtils.wrapSplit(i, nativeSplit, nativeSplit.getLocations()));
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
    @Nullable public static GridHadoopFileBlock readFileBlock(String clsName, FSDataInputStream in,
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

        return new GridHadoopFileBlock(hosts, split.getPath().toUri(), split.getStart(), split.getLength());
    }
}
