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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.hadoop.HadoopFileBlock;
import org.apache.ignite.hadoop.HadoopInputSplit;
import org.apache.ignite.internal.processors.hadoop.impl.HadoopUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

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