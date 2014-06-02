/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.router;

import org.gridgain.client.*;

import java.util.*;

/**
 * TCP router interface.
 * <p>
 * Router allows remote clients to connect to grid without direct access to
 * the network where grid is running. It accepts requests on the same protocol
 * as grid rest server and establish required connection to grid nodes
 * to serve them.
 * <p>
 * Below is an example on how to start TCP router with non-default configuration.
 * <pre name="code" class="java">
 * GridTcpRouterConfiguration cfg =
 *     new GridTcpRouterConfiguration();
 *
 * cfg.setHost("router.appdomain.com");
 * cfg.setPort(11211);
 *
 * cfg.setServers(Arrays.asList(
 *     "node1.appdomain.com:11211",
 *     "node2.appdomain.com:11211"));
 *
 * GridRouterFactory.startTcpRouter(cfg);
 * </pre>
 * <p>
 * Note that clients should be specifically configured in order to use router.
 * Please refer to {@link GridClientConfiguration#getServers()} and
 * {@link GridClientConfiguration#getRouters()} documentation for more details.
 * <p>
 * Instances of this interface are managed through {@link GridRouterFactory}.
 *
 * @see GridTcpRouterConfiguration
 *
 */
public interface GridTcpRouter {
    /**
     * Returns router Id.
     * <p>
     * Unique router Ids are automatically generated on router startup.
     * They are used to control router's lifecycle via {@link GridRouterFactory}.
     *
     * @see GridRouterFactory#tcpRouter(UUID)
     * @see GridRouterFactory#stopTcpRouter(UUID)
     * @return Router Id.
     */
    public UUID id();

    /**
     * Returns configuration used to start router.
     *
     * @see GridRouterFactory#startTcpRouter(GridTcpRouterConfiguration)
     * @return Router configuration.
     */
    public GridTcpRouterConfiguration configuration();
}
