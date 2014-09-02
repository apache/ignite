/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.router;

import org.gridgain.client.router.impl.*;
import org.gridgain.grid.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * This factory is responsible for router lifecycle management.
 * All router should be started, accessed and stopped through this factory.
 * <h1 class="header">Embedding router</h1>
 * You can use {@link GridTcpRouterConfiguration} to set configuration parameters and pass them to
 * {@link #startTcpRouter(GridTcpRouterConfiguration)}.
 * <p>
 * See {@link GridTcpRouter} for example on how to configure and start embedded router.
 * <h1 class="header">Standalone router startup</h1>
 * Alternatively you can run routers as a standalone processes by executing
 * {@code GRIDGAIN_HOME/bin/ggrouter.sh} or {@code GRIDGAIN_HOME/bin/ggrouter.bat}.
 * They both accept path to a configuration file as first command line argument.
 * See {@code GRIDGAIN_HOME/config/router/default-router.xml} for configuration example.
 *
 * @see GridTcpRouter
 */
public final class GridRouterFactory {
    /** Map of running TCP routers. */
    private static ConcurrentMap<UUID, GridTcpRouterImpl> tcpRouters =
        new ConcurrentHashMap<>();

    /**
     * Ensure singleton,
     */
    private GridRouterFactory() {
        // No-op.
    }

    /**
     * Starts a TCP router with given configuration.
     * <p>
     * Starting router will be assigned a randomly generated UUID which can be obtained
     * by {@link GridTcpRouter#id()} method. Later this instance could be obtained via
     * {@link #tcpRouter} method.
     *
     * @param cfg Router configuration.
     * @return Started router.
     * @throws GridException If router start failed.
     */
    public static GridTcpRouter startTcpRouter(GridTcpRouterConfiguration cfg) throws GridException {
        GridTcpRouterImpl router = new GridTcpRouterImpl(cfg);

        router.start();

        GridTcpRouterImpl old = tcpRouters.putIfAbsent(router.id(), router);

        assert old == null : "UUIDs collision happens [tcpRouters=" + tcpRouters + ", router=" + router + ']';

        return router;
    }

    /**
     * Stops particular TCP router.
     *
     * @param tcpRouterId Id of the router to stop.
     */
    public static void stopTcpRouter(UUID tcpRouterId) {
        GridTcpRouterImpl router = tcpRouters.remove(tcpRouterId);

        if (router != null)
            router.stop();
    }

    /**
     * Returns TCP router with the given id.
     *
     * @param id Router Id.
     * @return Router with the given id or {@code null} if router not found.
     */
    @Nullable public static GridTcpRouter tcpRouter(UUID id) {
        return tcpRouters.get(id);
    }

    /**
     * Returns collection of all currently running TCP routers.
     *
     * @return Collection of currently running {@link GridTcpRouter}s.
     */
    public static Collection<GridTcpRouter> allTcpRouters() {
        return new ArrayList<GridTcpRouter>(tcpRouters.values());
    }

    /**
     * Stops all currently active routers.
     */
    public static void stopAllRouters() {
        for (Iterator<GridTcpRouterImpl> it = tcpRouters.values().iterator(); it.hasNext(); ) {
            GridTcpRouterImpl router = it.next();

            it.remove();

            router.stop();
        }
    }
}
