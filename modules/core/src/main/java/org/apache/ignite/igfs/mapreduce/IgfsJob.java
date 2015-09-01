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

package org.apache.ignite.igfs.mapreduce;

import java.io.IOException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.igfs.IgfsInputStream;

/**
 * Defines executable unit for {@link IgfsTask}. Before this job is executed, it is assigned one of the
 * ranges provided by the {@link IgfsRecordResolver} passed to one of the {@code IgniteFs.execute(...)} methods.
 * <p>
 * {@link #execute(org.apache.ignite.IgniteFileSystem, IgfsFileRange, org.apache.ignite.igfs.IgfsInputStream)} method is given {@link IgfsFileRange} this
 * job is expected to operate on, and already opened {@link org.apache.ignite.igfs.IgfsInputStream} for the file this range belongs to.
 * <p>
 * Note that provided input stream has position already adjusted to range start. However, it will not
 * automatically stop on range end. This is done to provide capability in some cases to look beyond
 * the range end or seek position before the reange start.
 * <p>
 * In majority of the cases, when you want to process only provided range, you should explicitly control amount
 * of returned data and stop at range end. You can also use {@link IgfsInputStreamJobAdapter}, which operates
 * on {@link IgfsRangeInputStream} bounded to range start and end, or manually wrap provided input stream with
 * {@link IgfsRangeInputStream}.
 * <p>
 * You can inject any resources in concrete implementation, just as with regular {@link org.apache.ignite.compute.ComputeJob} implementations.
 */
public interface IgfsJob {
    /**
     * Executes this job.
     *
     * @param igfs IGFS instance.
     * @param range File range aligned to record boundaries.
     * @param in Input stream for split file. This input stream is not aligned to range and points to file start
     *     by default.
     * @return Execution result.
     * @throws IgniteException If execution failed.
     * @throws IOException If file system operation resulted in IO exception.
     */
    public Object execute(IgniteFileSystem igfs, IgfsFileRange range, IgfsInputStream in) throws IgniteException,
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