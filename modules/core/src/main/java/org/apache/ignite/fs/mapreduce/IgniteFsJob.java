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

package org.apache.ignite.fs.mapreduce;

import org.apache.ignite.*;
import org.apache.ignite.fs.*;

import java.io.*;

/**
 * Defines executable unit for {@link IgniteFsTask}. Before this job is executed, it is assigned one of the
 * ranges provided by the {@link IgniteFsRecordResolver} passed to one of the {@code GridGgfs.execute(...)} methods.
 * <p>
 * {@link #execute(org.apache.ignite.IgniteFs, IgniteFsFileRange, org.apache.ignite.fs.IgniteFsInputStream)} method is given {@link IgniteFsFileRange} this
 * job is expected to operate on, and already opened {@link org.apache.ignite.fs.IgniteFsInputStream} for the file this range belongs to.
 * <p>
 * Note that provided input stream has position already adjusted to range start. However, it will not
 * automatically stop on range end. This is done to provide capability in some cases to look beyond
 * the range end or seek position before the reange start.
 * <p>
 * In majority of the cases, when you want to process only provided range, you should explicitly control amount
 * of returned data and stop at range end. You can also use {@link IgniteFsInputStreamJobAdapter}, which operates
 * on {@link IgniteFsRangeInputStream} bounded to range start and end, or manually wrap provided input stream with
 * {@link IgniteFsRangeInputStream}.
 * <p>
 * You can inject any resources in concrete implementation, just as with regular {@link org.apache.ignite.compute.ComputeJob} implementations.
 */
public interface IgniteFsJob {
    /**
     * Executes this job.
     *
     * @param ggfs GGFS instance.
     * @param range File range aligned to record boundaries.
     * @param in Input stream for split file. This input stream is not aligned to range and points to file start
     *     by default.
     * @return Execution result.
     * @throws IgniteCheckedException If execution failed.
     * @throws IOException If file system operation resulted in IO exception.
     */
    public Object execute(IgniteFs ggfs, IgniteFsFileRange range, IgniteFsInputStream in) throws IgniteCheckedException,
        IOException;

    /**
     * This method is called when system detects that completion of this
     * job can no longer alter the overall outcome (for example, when parent task
     * has already reduced the results). Job is also cancelled when
     * {@link org.apache.ignite.compute.ComputeTaskFuture#cancel()} is called.
     * <p>
     * Note that job cancellation is only a hint, and just like with
     * {@link Thread#interrupt()}  method, it is really up to the actual job
     * instance to gracefully finish execution and exit.
     */
    public void cancel();
}
