/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.ggfs.mapreduce;

import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.ggfs.*;

import java.io.*;

/**
 * Defines executable unit for {@link GridGgfsTask}. Before this job is executed, it is assigned one of the
 * ranges provided by the {@link GridGgfsRecordResolver} passed to one of the {@code GridGgfs.execute(...)} methods.
 * <p>
 * {@link #execute(org.apache.ignite.IgniteFs, IgniteFsFileRange, org.gridgain.grid.ggfs.IgniteFsInputStream)} method is given {@link IgniteFsFileRange} this
 * job is expected to operate on, and already opened {@link org.gridgain.grid.ggfs.IgniteFsInputStream} for the file this range belongs to.
 * <p>
 * Note that provided input stream has position already adjusted to range start. However, it will not
 * automatically stop on range end. This is done to provide capability in some cases to look beyond
 * the range end or seek position before the reange start.
 * <p>
 * In majority of the cases, when you want to process only provided range, you should explicitly control amount
 * of returned data and stop at range end. You can also use {@link IgniteFsInputStreamJobAdapter}, which operates
 * on {@link GridGgfsRangeInputStream} bounded to range start and end, or manually wrap provided input stream with
 * {@link GridGgfsRangeInputStream}.
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
     * @throws GridException If execution failed.
     * @throws IOException If file system operation resulted in IO exception.
     */
    public Object execute(IgniteFs ggfs, IgniteFsFileRange range, IgniteFsInputStream in) throws GridException,
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
