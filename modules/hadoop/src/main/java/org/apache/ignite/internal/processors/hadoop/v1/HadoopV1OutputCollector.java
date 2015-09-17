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