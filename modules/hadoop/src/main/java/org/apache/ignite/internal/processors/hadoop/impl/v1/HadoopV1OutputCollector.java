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

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.TaskAttemptContextImpl;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskContext;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;

/**
 * Hadoop output collector.
 */
public class HadoopV1OutputCollector implements OutputCollector {
    /** Job configuration. */
    private final JobConf jobConf;

    /** Task context. */
    private final HadoopTaskContext taskCtx;

    /** Optional direct writer. */
    private final RecordWriter writer;

    /** Task attempt. */
    private final TaskAttemptID attempt;

    /**
     * @param jobConf Job configuration.
     * @param taskCtx Task context.
     * @param directWrite Direct write flag.
     * @param fileName File name.
     * @throws IOException In case of IO exception.
     */
    HadoopV1OutputCollector(JobConf jobConf, HadoopTaskContext taskCtx, boolean directWrite,
        @Nullable String fileName, TaskAttemptID attempt) throws IOException {
        this.jobConf = jobConf;
        this.taskCtx = taskCtx;
        this.attempt = attempt;

        if (directWrite) {
            jobConf.set("mapreduce.task.attempt.id", attempt.toString());

            OutputFormat outFormat = jobConf.getOutputFormat();

            writer = outFormat.getRecordWriter(null, jobConf, fileName, Reporter.NULL);
        }
        else
            writer = null;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void collect(Object key, Object val) throws IOException {
        if (writer != null)
            writer.write(key, val);
        else {
            try {
                taskCtx.output().write(key, val);
            }
            catch (IgniteCheckedException e) {
                throw new IOException(e);
            }
        }
    }

    /**
     * Close writer.
     *
     * @throws IOException In case of IO exception.
     */
    public void closeWriter() throws IOException {
        if (writer != null)
            writer.close(Reporter.NULL);
    }

    /**
     * Setup task.
     *
     * @throws IOException If failed.
     */
    public void setup() throws IOException {
        if (writer != null)
            jobConf.getOutputCommitter().setupTask(new TaskAttemptContextImpl(jobConf, attempt));
    }

    /**
     * Commit task.
     *
     * @throws IOException In failed.
     */
    public void commit() throws IOException {
        if (writer != null) {
            OutputCommitter outputCommitter = jobConf.getOutputCommitter();

            TaskAttemptContext taskCtx = new TaskAttemptContextImpl(jobConf, attempt);

            if (outputCommitter.needsTaskCommit(taskCtx))
                outputCommitter.commitTask(taskCtx);
        }
    }

    /**
     * Abort task.
     */
    public void abort() {
        try {
            if (writer != null)
                jobConf.getOutputCommitter().abortTask(new TaskAttemptContextImpl(jobConf, attempt));
        }
        catch (IOException ignore) {
            // No-op.
        }
    }
}