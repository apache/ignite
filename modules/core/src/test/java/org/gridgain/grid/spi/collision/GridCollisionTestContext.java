/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.collision;

import java.util.*;

/**
 * Tes
 */
public class GridCollisionTestContext implements GridCollisionContext {
    /** Active jobs. */
    private Collection<GridCollisionJobContext> activeJobs;

    /** Wait jobs. */
    private Collection<GridCollisionJobContext> waitJobs;

    /** Held jobs. */
    private Collection<GridCollisionJobContext> heldJobs;

    /**
     * @param activeJobs Active jobs.
     * @param waitJobs Waiting jobs.
     */
    public GridCollisionTestContext(Collection<GridCollisionJobContext> activeJobs,
        Collection<GridCollisionJobContext> waitJobs) {
        this.activeJobs = activeJobs;
        this.waitJobs = waitJobs;
    }

    /**
     * @param activeJobs Active jobs.
     * @param waitJobs Waiting jobs.
     * @param heldJobs Held jobs.
     */
    public GridCollisionTestContext(Collection<GridCollisionJobContext> activeJobs,
        Collection<GridCollisionJobContext> waitJobs,
        Collection<GridCollisionJobContext> heldJobs) {
        this.activeJobs = activeJobs;
        this.waitJobs = waitJobs;
        this.heldJobs = heldJobs;
    }

    /** {@inheritDoc} */
    @Override public Collection<GridCollisionJobContext> activeJobs() {
        return mask(activeJobs);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridCollisionJobContext> heldJobs() {
        return mask(heldJobs);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridCollisionJobContext> waitingJobs() {
        return mask(waitJobs);
    }

    /**
     * @param c Collection to check for {@code null}.
     * @return Non-null collection.
     */
    private Collection<GridCollisionJobContext> mask(Collection<GridCollisionJobContext> c) {
        return c == null ? Collections.<GridCollisionJobContext>emptyList() : c;
    }
}
