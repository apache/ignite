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

package org.apache.ignite.internal.processors.hadoop.impl.delegate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.hadoop.fs.IgniteHadoopFileSystemCounterWriter;
import org.apache.ignite.internal.processors.hadoop.HadoopDefaultJobInfo;
import org.apache.ignite.internal.processors.hadoop.HadoopJobEx;
import org.apache.ignite.internal.processors.hadoop.HadoopJobId;
import org.apache.ignite.internal.processors.hadoop.HadoopJobInfo;
import org.apache.ignite.internal.processors.hadoop.counter.HadoopCounters;
import org.apache.ignite.internal.processors.hadoop.counter.HadoopPerformanceCounter;
import org.apache.ignite.internal.processors.hadoop.delegate.HadoopFileSystemCounterWriterDelegate;
import org.apache.ignite.internal.processors.hadoop.impl.HadoopUtils;
import org.apache.ignite.internal.processors.hadoop.impl.v2.HadoopV2Job;
import org.apache.ignite.internal.processors.igfs.IgfsUtils;
import org.apache.ignite.internal.util.typedef.T2;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;

/**
 * Counter writer delegate implementation.
 */
@SuppressWarnings("unused")
public class HadoopFileSystemCounterWriterDelegateImpl implements HadoopFileSystemCounterWriterDelegate {
    /** */
    private static final String USER_MACRO = "${USER}";

    /** */
    private static final String DEFAULT_COUNTER_WRITER_DIR = "/user/" + USER_MACRO;

    /**
     * Constructor.
     *
     * @param proxy Proxy (not used).
     */
    public HadoopFileSystemCounterWriterDelegateImpl(IgniteHadoopFileSystemCounterWriter proxy) {
        // No-op.
    }

    /** {@inheritDoc} */
    public void write(HadoopJobEx job, HadoopCounters cntrs) throws IgniteCheckedException {
        Configuration hadoopCfg = HadoopUtils.safeCreateConfiguration();

        final HadoopJobInfo jobInfo = job.info();

        final HadoopJobId jobId = job.id();

        for (Map.Entry<String, String> e : ((HadoopDefaultJobInfo)jobInfo).properties().entrySet())
            hadoopCfg.set(e.getKey(), e.getValue());

        String user = jobInfo.user();

        user = IgfsUtils.fixUserName(user);

        String dir = jobInfo.property(IgniteHadoopFileSystemCounterWriter.COUNTER_WRITER_DIR_PROPERTY);

        if (dir == null)
            dir = DEFAULT_COUNTER_WRITER_DIR;

        Path jobStatPath = new Path(new Path(dir.replace(USER_MACRO, user)), jobId.toString());

        HadoopPerformanceCounter perfCntr = HadoopPerformanceCounter.getCounter(cntrs, null);

        try {
            hadoopCfg.set(MRJobConfig.USER_NAME, user);

            FileSystem fs = ((HadoopV2Job)job).fileSystem(jobStatPath.toUri(), hadoopCfg);

            fs.mkdirs(jobStatPath);

            try (PrintStream out = new PrintStream(fs.create(
                new Path(jobStatPath, IgniteHadoopFileSystemCounterWriter.PERFORMANCE_COUNTER_FILE_NAME)))) {
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
