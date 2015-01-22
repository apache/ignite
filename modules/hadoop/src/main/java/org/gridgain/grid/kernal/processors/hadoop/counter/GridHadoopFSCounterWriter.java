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

package org.gridgain.grid.kernal.processors.hadoop.counter;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.util.typedef.*;

import java.io.*;
import java.util.*;

/**
 * Statistic writer implementation that writes info into any Hadoop file system.
 */
public class GridHadoopFSCounterWriter implements GridHadoopCounterWriter {
    /** */
    public static final String PERFORMANCE_COUNTER_FILE_NAME = "performance";

    /** */
    private static final String DEFAULT_USER_NAME = "anonymous";

    /** */
    public static final String COUNTER_WRITER_DIR_PROPERTY = "gridgain.counters.fswriter.directory";

    /** */
    private static final String USER_MACRO = "${USER}";

    /** */
    private static final String DEFAULT_COUNTER_WRITER_DIR = "/user/" + USER_MACRO;

    /** {@inheritDoc} */
    @Override public void write(GridHadoopJobInfo jobInfo, GridHadoopJobId jobId, GridHadoopCounters cntrs)
        throws IgniteCheckedException {

        Configuration hadoopCfg = new Configuration();

        for (Map.Entry<String, String> e : ((GridHadoopDefaultJobInfo)jobInfo).properties().entrySet())
            hadoopCfg.set(e.getKey(), e.getValue());

        String user = jobInfo.user();

        if (F.isEmpty(user))
            user = DEFAULT_USER_NAME;

        String dir = jobInfo.property(COUNTER_WRITER_DIR_PROPERTY);

        if (dir == null)
            dir = DEFAULT_COUNTER_WRITER_DIR;

        Path jobStatPath = new Path(new Path(dir.replace(USER_MACRO, user)), jobId.toString());

        GridHadoopPerformanceCounter perfCntr = GridHadoopPerformanceCounter.getCounter(cntrs, null);

        try {
            FileSystem fs = jobStatPath.getFileSystem(hadoopCfg);

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
