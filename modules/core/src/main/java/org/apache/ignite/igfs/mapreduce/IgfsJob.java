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