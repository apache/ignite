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
import org.gridgain.grid.util.*;

import java.io.*;

/**
 * Convenient {@link GridGgfsJob} adapter. It limits data returned from {@link GridGgfsInputStream} to bytes within
 * the {@link GridGgfsFileRange} assigned to the job.
 * <p>
 * Under the covers it simply puts job's {@code GridGgfsInputStream} position to range start and wraps in into
 * {@link GridFixedSizeInputStream} limited to range length.
 */
public abstract class GridGgfsInputStreamJobAdapter extends GridGgfsJobAdapter {
    /** {@inheritDoc} */
    @Override public final Object execute(IgniteFs ggfs, GridGgfsFileRange range, GridGgfsInputStream in)
        throws GridException, IOException {
        in.seek(range.start());

        return execute(ggfs, new GridGgfsRangeInputStream(in, range));
    }

    /**
     * Executes this job.
     *
     * @param ggfs GGFS instance.
     * @param in Input stream.
     * @return Execution result.
     * @throws GridException If execution failed.
     * @throws IOException If IO exception encountered while working with stream.
     */
    public abstract Object execute(IgniteFs ggfs, GridGgfsRangeInputStream in) throws GridException, IOException;
}
