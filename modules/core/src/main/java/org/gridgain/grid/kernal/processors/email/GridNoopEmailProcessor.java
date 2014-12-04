/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.email;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.future.*;

import java.util.*;

/**
 * No-op implementation of {@code GridEmailProcessorAdapter}.
 */
public class GridNoopEmailProcessor extends GridEmailProcessorAdapter {
    /**
     * @param ctx Kernal context.
     */
    public GridNoopEmailProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void sendNow(String subj, String body, boolean html) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void sendNow(String subj, String body, boolean html, Collection<String> addrs) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> schedule(String subj, String body, boolean html) {
        return new GridFinishedFuture<>(ctx, true);
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Boolean> schedule(String subj, String body, boolean html, Collection<String> addrs) {
        return new GridFinishedFuture<>(ctx, true);
    }
}
