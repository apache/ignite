/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.service;

import org.apache.ignite.managed.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Service deployment.
 */
public class GridServiceDeployment implements GridCacheInternal, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Node ID. */
    private UUID nodeId;

    /** Service configuration. */
    private ManagedServiceConfiguration cfg;

    /**
     * @param nodeId Node ID.
     * @param cfg Service configuration.
     */
    public GridServiceDeployment(UUID nodeId, ManagedServiceConfiguration cfg) {
        this.nodeId = nodeId;
        this.cfg = cfg;
    }

    /**
     * @return Node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Service configuration.
     */
    public ManagedServiceConfiguration configuration() {
        return cfg;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        GridServiceDeployment that = (GridServiceDeployment)o;

        if (cfg != null ? !cfg.equals(that.cfg) : that.cfg != null)
            return false;

        if (nodeId != null ? !nodeId.equals(that.nodeId) : that.nodeId != null)
            return false;

        return true;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = nodeId != null ? nodeId.hashCode() : 0;

        res = 31 * res + (cfg != null ? cfg.hashCode() : 0);

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridServiceDeployment.class, this);
    }
}
