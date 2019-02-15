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

package org.apache.ignite.internal.processors.hadoop.impl.v2;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.hadoop.HadoopTask;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskContext;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskInfo;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;

/**
 * Extended Hadoop v2 task.
 */
public abstract class HadoopV2Task extends HadoopTask {
    /** Hadoop context. */
    private HadoopV2Context hadoopCtx;

    /**
     * Constructor.
     *
     * @param taskInfo Task info.
     */
    protected HadoopV2Task(HadoopTaskInfo taskInfo) {
        super(taskInfo);
    }

    /** {@inheritDoc} */
    @Override public void run(HadoopTaskContext taskCtx) throws IgniteCheckedException {
        HadoopV2TaskContext ctx = (HadoopV2TaskContext)taskCtx;

        hadoopCtx = new HadoopV2Context(ctx);

        run0(ctx);
    }

    /**
     * Internal task routine.
     *
     * @param taskCtx Task context.
     * @throws IgniteCheckedException
     */
    protected abstract void run0(HadoopV2TaskContext taskCtx) throws IgniteCheckedException;

    /**
     * @return hadoop context.
     */
    protected HadoopV2Context hadoopContext() {
        return hadoopCtx;
    }

    /**
     * Create and configure an OutputFormat instance.
     *
     * @param jobCtx Job context.
     * @return Instance of OutputFormat is specified in job configuration.
     * @throws ClassNotFoundException If specified class not found.
     */
    protected OutputFormat getOutputFormat(JobContext jobCtx) throws ClassNotFoundException {
        return ReflectionUtils.newInstance(jobCtx.getOutputFormatClass(), hadoopContext().getConfiguration());
    }

    /**
     * Put write into Hadoop context and return associated output format instance.
     *
     * @param jobCtx Job context.
     * @return Output format.
     * @throws IgniteCheckedException In case of Grid exception.
     * @throws InterruptedException In case of interrupt.
     */
    protected OutputFormat prepareWriter(JobContext jobCtx)
        throws IgniteCheckedException, InterruptedException {
        try {
            OutputFormat outputFormat = getOutputFormat(jobCtx);

            assert outputFormat != null;

            OutputCommitter outCommitter = outputFormat.getOutputCommitter(hadoopCtx);

            if (outCommitter != null)
                outCommitter.setupTask(hadoopCtx);

            RecordWriter writer = outputFormat.getRecordWriter(hadoopCtx);

            hadoopCtx.writer(writer);

            return outputFormat;
        }
        catch (IOException | ClassNotFoundException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * Closes writer.
     *
     * @throws Exception If fails and logger hasn't been specified.
     */
    protected void closeWriter() throws Exception {
        RecordWriter writer = hadoopCtx.writer();

        if (writer != null)
            writer.close(hadoopCtx);
    }

    /**
     * Setup task.
     *
     * @param outputFormat Output format.
     * @throws IOException In case of IO exception.
     * @throws InterruptedException In case of interrupt.
     */
    protected void setup(@Nullable OutputFormat outputFormat) throws IOException, InterruptedException {
        if (hadoopCtx.writer() != null) {
            assert outputFormat != null;

            outputFormat.getOutputCommitter(hadoopCtx).setupTask(hadoopCtx);
        }
    }

    /**
     * Commit task.
     *
     * @param outputFormat Output format.
     * @throws IgniteCheckedException In case of Grid exception.
     * @throws IOException In case of IO exception.
     * @throws InterruptedException In case of interrupt.
     */
    protected void commit(@Nullable OutputFormat outputFormat) throws IgniteCheckedException, IOException, InterruptedException {
        if (hadoopCtx.writer() != null) {
            assert outputFormat != null;

            OutputCommitter outputCommitter = outputFormat.getOutputCommitter(hadoopCtx);

            if (outputCommitter.needsTaskCommit(hadoopCtx))
                outputCommitter.commitTask(hadoopCtx);
        }
    }

    /**
     * Abort task.
     *
     * @param outputFormat Output format.
     */
    protected void abort(@Nullable OutputFormat outputFormat) {
        if (hadoopCtx.writer() != null) {
            assert outputFormat != null;

            try {
                outputFormat.getOutputCommitter(hadoopCtx).abortTask(hadoopCtx);
            }
            catch (IOException ignore) {
                // Ignore.
            }
            catch (InterruptedException ignore) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void cancel() {
        hadoopCtx.cancel();
    }
}