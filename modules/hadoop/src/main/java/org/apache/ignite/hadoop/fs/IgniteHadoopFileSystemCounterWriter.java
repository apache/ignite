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

package org.apache.ignite.hadoop.fs;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.ignite.*;
import org.apache.ignite.internal.processors.hadoop.*;
import org.apache.ignite.internal.processors.hadoop.counter.*;
import org.apache.ignite.internal.processors.hadoop.counter.HadoopCounters;
import org.apache.ignite.internal.processors.igfs.*;
import org.apache.ignite.internal.util.typedef.*;

import java.io.*;
import java.util.*;

/**
 * Statistic writer implementation that writes info into any Hadoop file system.
 */
public class IgniteHadoopFileSystemCounterWriter implements HadoopCounterWriter {
    /** */
    public static final String PERFORMANCE_COUNTER_FILE_NAME = "performance";

    /** */
    public static final String COUNTER_WRITER_DIR_PROPERTY = "ignite.counters.fswriter.directory";

    /** */
    private static final String USER_MACRO = "${USER}";

    /** */
    private static final String DEFAULT_COUNTER_WRITER_DIR = "/user/" + USER_MACRO;

    /** {@inheritDoc} */
    @Override public void write(HadoopJobInfo jobInfo, HadoopJobId jobId, HadoopCounters cntrs)
        throws IgniteCheckedException {

        Configuration hadoopCfg = HadoopUtils.safeCreateConfiguration();

        for (Map.Entry<String, String> e : ((HadoopDefaultJobInfo)jobInfo).properties().entrySet())
            hadoopCfg.set(e.getKey(), e.getValue());

        String user = jobInfo.user();

        user = IgfsUtils.fixUserName(user);

        String dir = jobInfo.property(COUNTER_WRITER_DIR_PROPERTY);

        if (dir == null)
            dir = DEFAULT_COUNTER_WRITER_DIR;

        Path jobStatPath = new Path(new Path(dir.replace(USER_MACRO, user)), jobId.toString());

        HadoopPerformanceCounter perfCntr = HadoopPerformanceCounter.getCounter(cntrs, null);

        try {
            hadoopCfg.set(MRJobConfig.USER_NAME, user);

            FileSystem fs = HadoopUtils.fileSystemForMrUser(jobStatPath.toUri(), hadoopCfg, true);

            fs.mkdirs(jobStatPath);

            try (PrintStream out = new PrintStream(fs.create(new Path(jobStatPath, PERFORMANCE_COUNTER_FILE_NAME)))) {
                for (T2<String, Long> evt : perfCntr.evts()) {
                    out.print(evt.get1());
                    out.print(':');
                    out.println(evt.get2().toString());
                }

                out.flush();
            }
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
    }
}
