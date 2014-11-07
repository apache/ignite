/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.service;

import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Service execution context. Execution context is provided into {@link GridService#execute(GridServiceContext)}
 * and {@link GridService#cancel(GridServiceContext)} methods and contains information about specific service
 * execution.
 */
public interface GridServiceContext extends Serializable {
    /**
     * Gets service name.
     *
     * @return Service name.
     */
    public String name();

    /**
     * Gets service execution ID. Execution ID is guaranteed to be unique across
     * all service deployments.
     *
     * @return Service execution ID.
     */
    public UUID executionId();

    /**
     * Get flag indicating whether service has been cancelled or not.
     *
     * @return Flag indicating whether service has been cancelled or not.
     */
    public boolean isCancelled();

    /**
     * Gets cache name used for key-to-node affinity calculation. This parameter is optional
     * and is set only when key-affinity service was deployed.
     *
     * @return Cache name, possibly {@code null}.
     */
    @Nullable public String cacheName();

    /**
     * Gets affinity key used for key-to-node affinity calculation. This parameter is optional
     * and is set only when key-affinity service was deployed.
     *
     * @return Affinity key, possibly {@code null}.
     */
    @Nullable public <K> K affinityKey();
}
