/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor;

import org.apache.ignite.compute.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import static org.gridgain.grid.kernal.visor.util.VisorTaskUtils.*;

/**
 * Base class for Visor jobs.
 */
public abstract class VisorJob<A, R> extends ComputeJobAdapter {
    @IgniteInstanceResource
    protected GridEx g;

    /** Job start time. */
    protected long start;

    /** Debug flag. */
    protected boolean debug;

    /**
     * Create job with specified argument.
     *
     * @param arg Job argument.
     */
    protected VisorJob(@Nullable A arg, boolean debug) {
        super(arg);

        this.debug = debug;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object execute() throws IgniteCheckedException {
        start = U.currentTimeMillis();

        A arg = argument(0);

        try {
            if (debug)
                logStart(g.log(), getClass(), start);

            return run(arg);
        }
        finally {
            if (debug)
                logFinish(g.log(), getClass(), start);
        }
    }

    /**
     * Execution logic of concrete task.
     *
     * @return Result.
     */
    protected abstract R run(@Nullable A arg) throws IgniteCheckedException;
}
