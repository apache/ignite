// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.security;

import org.gridgain.grid.spi.authentication.*;

import java.util.*;

/**
 * Security context.
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridSecurityContext {
    /**
     * Gets collection of operations requested to execute.
     *
     * @return Collection of operations requested to execute.
     */
    public Collection<GridSecurityOperation> operations();

    /**
     * Gets cache name if cache operation is being checked.
     *
     * @return Optional cache name.
     */
    public String cacheName();

    /**
     * Gets task name being executed if task execution is checked.
     *
     * @return Task name.
     */
    public String taskName();

    /**
     * Authentication subject context returned by authentication SPI.
     *
     * @return Authentication subject context.
     */
    public GridSecuritySubject subject();
}
