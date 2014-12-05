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
import org.apache.ignite.fs.*;
import org.gridgain.grid.ggfs.mapreduce.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.ipc.*;
import org.gridgain.grid.util.typedef.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Nop GGFS processor implementation.
 */
public class GridNoopGgfsProcessor extends GridGgfsProcessorAdapter {
    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */
    public GridNoopGgfsProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>>");
        X.println(">>> GGFS processor memory stats [grid=" + ctx.gridName() + ']');
        X.println(">>>   ggfsCacheSize: " + 0);
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteFs> ggfss() {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteFs ggfs(@Nullable String name) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<GridIpcServerEndpoint> endpoints(@Nullable String name) {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Nullable @Override public ComputeJob createJob(IgniteFsJob job, @Nullable String ggfsName, IgniteFsPath path,
        long start, long length, IgniteFsRecordResolver recRslv) {
        return null;
    }
}
