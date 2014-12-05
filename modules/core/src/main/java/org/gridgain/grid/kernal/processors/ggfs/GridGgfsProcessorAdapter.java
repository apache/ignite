/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.ggfs.mapreduce.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.*;
import org.gridgain.grid.util.ipc.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * GGFS processor adapter.
 */
public abstract class GridGgfsProcessorAdapter extends GridProcessorAdapter {
    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */
    protected GridGgfsProcessorAdapter(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * Gets all GGFS instances.
     *
     * @return Collection of GGFS instances.
     */
    public abstract Collection<IgniteFs> ggfss();

    /**
     * Gets GGFS instance.
     *
     * @param name (Nullable) GGFS name.
     * @return GGFS instance.
     */
    @Nullable public abstract IgniteFs ggfs(@Nullable String name);

    /**
     * Gets server endpoints for particular GGFS.
     *
     * @param name GGFS name.
     * @return Collection of endpoints or {@code null} in case GGFS is not defined.
     */
    public abstract Collection<GridIpcServerEndpoint> endpoints(@Nullable String name);

    /**
     * Create compute job for the given GGFS job.
     *
     * @param job GGFS job.
     * @param ggfsName GGFS name.
     * @param path Path.
     * @param start Start position.
     * @param length Length.
     * @param recRslv Record resolver.
     * @return Compute job.
     */
    @Nullable public abstract ComputeJob createJob(IgniteFsJob job, @Nullable String ggfsName, IgniteFsPath path,
        long start, long length, IgniteFsRecordResolver recRslv);
}
