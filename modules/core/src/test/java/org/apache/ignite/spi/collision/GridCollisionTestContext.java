/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.collision;

import java.util.*;

/**
 * Tes
 */
public class GridCollisionTestContext implements CollisionContext {
    /** Active jobs. */
    private Collection<CollisionJobContext> activeJobs;

    /** Wait jobs. */
    private Collection<CollisionJobContext> waitJobs;

    /** Held jobs. */
    private Collection<CollisionJobContext> heldJobs;

    /**
     * @param activeJobs Active jobs.
     * @param waitJobs Waiting jobs.
     */
    public GridCollisionTestContext(Collection<CollisionJobContext> activeJobs,
        Collection<CollisionJobContext> waitJobs) {
        this.activeJobs = activeJobs;
        this.waitJobs = waitJobs;
    }

    /**
     * @param activeJobs Active jobs.
     * @param waitJobs Waiting jobs.
     * @param heldJobs Held jobs.
     */
    public GridCollisionTestContext(Collection<CollisionJobContext> activeJobs,
        Collection<CollisionJobContext> waitJobs,
        Collection<CollisionJobContext> heldJobs) {
        this.activeJobs = activeJobs;
        this.waitJobs = waitJobs;
        this.heldJobs = heldJobs;
    }

    /** {@inheritDoc} */
    @Override public Collection<CollisionJobContext> activeJobs() {
        return mask(activeJobs);
    }

    /** {@inheritDoc} */
    @Override public Collection<CollisionJobContext> heldJobs() {
        return mask(heldJobs);
    }

    /** {@inheritDoc} */
    @Override public Collection<CollisionJobContext> waitingJobs() {
        return mask(waitJobs);
    }

    /**
     * @param c Collection to check for {@code null}.
     * @return Non-null collection.
     */
    private Collection<CollisionJobContext> mask(Collection<CollisionJobContext> c) {
        return c == null ? Collections.<CollisionJobContext>emptyList() : c;
    }
}
