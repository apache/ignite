/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.client.router.impl;

import java.net.URL;
import java.util.Collection;
import java.util.Map;
import java.util.logging.Handler;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.client.router.GridTcpRouterConfiguration;
import org.apache.ignite.internal.util.spring.IgniteSpringHelper;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lifecycle.LifecycleAware;

import static org.apache.ignite.internal.IgniteComponentType.SPRING;
import static org.apache.ignite.internal.IgniteVersionUtils.ACK_VER_STR;
import static org.apache.ignite.internal.IgniteVersionUtils.COPYRIGHT;

/**
 * Loader class for router.
 */
public class GridRouterCommandLineStartup {
    /** Logger. */
    private IgniteLogger log;

    /** TCP router. */
    private LifecycleAware tcpRouter;

    /**
     * Search given context for required configuration and starts router.
     *
     * @param beans Beans loaded from spring configuration file.
     */
    public void start(Map<Class<?>, Object> beans) {
        log = (IgniteLogger)beans.get(IgniteLogger.class);

        if (log == null) {
            U.error(log, "Failed to find logger definition in application context. Stopping the router.");

            return;
        }

        GridTcpRouterConfiguration tcpCfg = (GridTcpRouterConfiguration)beans.get(GridTcpRouterConfiguration.class);

        if (tcpCfg == null)
            U.warn(log, "TCP router startup skipped (configuration not found).");
        else {
            tcpRouter = new GridTcpRouterImpl(tcpCfg);

            try {
                tcpRouter.start();
            }
            catch (Exception e) {
                U.error(log, "Failed to start TCP router on port " + tcpCfg.getPort() + ": " + e.getMessage(), e);

                tcpRouter = null;
            }
        }
    }

    /**
     * Stops router.
     */
    public void stop() {
        if (tcpRouter != null) {
            try {
                tcpRouter.stop();
            }
            catch (Exception e) {
                U.error(log, "Error while stopping the router.", e);
            }
        }
    }

    /**
     * Wrapper method to run router from command-line.
     *
     * @param args Command-line arguments.
     * @throws IgniteCheckedException If failed.
     */
    public static void main(String[] args) throws IgniteCheckedException {
        X.println(
            "   __________  ________________ ",
            "  /  _/ ___/ |/ /  _/_  __/ __/ ",
            " _/ // (_ /    // /  / / / _/   ",
            "/___/\\___/_/|_/___/ /_/ /___/  ",
            " ",
            "Ignite Router Command Line Loader",
            "ver. " + ACK_VER_STR,
            COPYRIGHT,
            " "
        );

        IgniteSpringHelper spring = SPRING.create(false);

        if (args.length < 1) {
            X.error("Missing XML configuration path.");

            System.exit(1);
        }

        String cfgPath = args[0];

        URL cfgUrl = U.resolveIgniteUrl(cfgPath);

        if (cfgUrl == null) {
            X.error("Spring XML file not found (is IGNITE_HOME set?): " + cfgPath);

            System.exit(1);
        }

        boolean isLog4jUsed = U.gridClassLoader().getResource("org/apache/log4j/Appender.class") != null;

        IgniteBiTuple<Object, Object> t = null;
        Collection<Handler> savedHnds = null;

        if (isLog4jUsed) {
            try {
                t = U.addLog4jNoOpLogger();
            }
            catch (Exception ignored) {
                isLog4jUsed = false;
            }
        }

        if (!isLog4jUsed)
            savedHnds = U.addJavaNoOpLogger();

        Map<Class<?>, Object> beans;

        try {
            beans = spring.loadBeans(cfgUrl, IgniteLogger.class, GridTcpRouterConfiguration.class);
        }
        finally {
            if (isLog4jUsed && t != null)
                U.removeLog4jNoOpLogger(t);

            if (!isLog4jUsed)
                U.removeJavaNoOpLogger(savedHnds);
        }

        final GridRouterCommandLineStartup routerStartup = new GridRouterCommandLineStartup();

        routerStartup.start(beans);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override public void run() {
                routerStartup.stop();
            }
        });
    }
}
