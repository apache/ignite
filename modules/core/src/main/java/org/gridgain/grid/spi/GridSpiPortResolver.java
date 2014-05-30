/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi;

import org.gridgain.grid.*;
import java.util.*;

/**
 * Provides resolution between external and internal ports. In some cases network
 * routers are configured to perform port mapping between external and internal networks and
 * the same mapping must be available to SPIs in GridGain that perform communication over
 * IP protocols.
 */
public interface GridSpiPortResolver extends GridMetadataAware {
    /**
     * Maps internal port to a collection of external ports. Note that you should not confuse the following:
     * <ul>
     * <li>Network Address Translation (NAT)
     * <li>Port Forwarding
     * <li>Port Mapping
     * </ul>
     * While port mapping will require the usage of this resolver the NAT and Port Forwarding are usually
     * done automatically and won't require any manual port resolution in most cases. Please
     * consult with your network administrator if you are unsure on whether or not and how to use
     * this resolver.
     *
     * @param port Internal (local) port.
     * @return Collection of ports that this local port is "known" outside. Note that if there are
     *      more than one external network the local port can be mapped differently to each and
     *      therefore may need to return multiple external ports.
     * @throws GridException Thrown if any exception occurs.
     */
    public Collection<Integer> getExternalPorts(int port) throws GridException;
}
