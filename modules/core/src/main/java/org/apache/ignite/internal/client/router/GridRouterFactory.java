/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.client.router;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.client.router.impl.GridTcpRouterImpl;
import org.jetbrains.annotations.Nullable;

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
 * {@code IGNITE_HOME/bin/igniterouter.sh} or {@code IGNITE_HOME/bin/igniterouter.bat}.
 * They both accept path to a configuration file as first command line argument.
 * See {@code IGNITE_HOME/config/router/default-router.xml} for configuration example.
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
     * @throws IgniteCheckedException If router start failed.
     */
    public static GridTcpRouter startTcpRouter(GridTcpRouterConfiguration cfg) throws IgniteCheckedException {
        GridTcpRouterImpl router = new GridTcpRouterImpl(cfg);

        try {
            router.start();
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to start router: " + e, e);
        }

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