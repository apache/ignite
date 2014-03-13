/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.router;

import org.gridgain.grid.util.mbean.*;

import java.util.*;

/**
 * MBean interface for HTTP rest router.
 */
@GridMBeanDescription("MBean for TCP router.")
public interface GridHttpRouterMBean {
    /**
     * Gets host for HTTP server.
     *
     * @return TCP host.
     */
    @GridMBeanDescription("Host for HTTP server.")
    public String getHost();

    /**
     * Gets port for HTTP server.
     *
     * @return TCP port.
     */
    @GridMBeanDescription("Port for HTTP server.")
    public int getPort();

    /**
     * Gets list of server addresses where router's embedded client should connect.
     *
     * @return List of server addresses.
     */
    @GridMBeanDescription("Gets list of server addresses where router's embedded client should connect.")
    public Collection<String> getServers();

    /**
     * Gets number of requests served by this router.
     *
     * @return Number of requests served by this router.
     */
    @GridMBeanDescription("Gets number of requests served by this router.")
    public long getRequestsCount();
}
