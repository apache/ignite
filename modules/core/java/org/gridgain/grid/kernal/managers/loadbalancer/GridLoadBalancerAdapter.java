// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.managers.loadbalancer;

import org.gridgain.grid.compute.*;
import org.gridgain.grid.util.lang.*;

import java.io.*;

/**
 * This adapter allows for graceful exception when load balancer gets marshalled.
 *
 * @author @java.author
 * @version @java.version
 */
public abstract class GridLoadBalancerAdapter extends GridMetadataAwareAdapter implements GridComputeLoadBalancer,
    Externalizable {
    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        // No-op.
    }

    /**
     * Reconstructs object on demarshalling.
     *
     * @return Reconstructed object.
     * @throws ObjectStreamException Thrown in case of demarshalling error.
     */
    protected Object readResolve() throws ObjectStreamException {
        throw new InvalidObjectException("Load balancer cannot be used from remote node.");
    }
}
