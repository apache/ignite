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

package org.apache.ignite.internal.processors.hadoop.v2;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.hadoop.HadoopFileBlock;
import org.apache.ignite.internal.processors.hadoop.HadoopInputSplit;
import org.apache.ignite.internal.processors.hadoop.HadoopUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Hadoop API v2 splitter.
 */
public class HadoopV2Splitter {
    /** */
    private static final String[] EMPTY_HOSTS = {};

    /**
     * @param ctx Job context.
     * @return Collection of mapped splits.
     * @throws IgniteCheckedException If mapping failed.
     */
    public static Collection<HadoopInputSplit> splitJob(JobContext ctx) throws IgniteCheckedException {
        try {
            InputFormat<?, ?> format = ReflectionUtils.newInstance(ctx.getInputFormatClass(), ctx.getConfiguration());

            assert format != null;

            List<InputSplit> splits = format.getSplits(ctx);

            Collection<HadoopInputSplit> res = new ArrayList<>(splits.size());

            int id = 0;

            for (InputSplit nativeSplit : splits) {
                if (nativeSplit instanceof FileSplit) {
                    FileSplit s = (FileSplit)nativeSplit;

                    res.add(new HadoopFileBlock(s.getLocations(), s.getPath().toUri(), s.getStart(), s.getLength()));
                }
                else
                    res.add(HadoopUtils.wrapSplit(id, nativeSplit, nativeSplit.getLocations()));

                id++;
            }

            return res;
        }
        catch (IOException | ClassNotFoundException e) {
            throw new IgniteCheckedException(e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedCheckedException(e);
        }
    }

    /**
     * @param clsName Input split class name.
     * @param in Input stream.
     * @param hosts Optional hosts.
     * @return File block or {@code null} if it is not a {@link FileSplit} instance.
     * @throws IgniteCheckedException If failed.
     */
    public static HadoopFileBlock readFileBlock(String clsName, DataInput in, @Nullable String[] hosts)
        throws IgniteCheckedException {
        if (!FileSplit.class.getName().equals(clsName))
            return null;

        FileSplit split = new FileSplit();

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