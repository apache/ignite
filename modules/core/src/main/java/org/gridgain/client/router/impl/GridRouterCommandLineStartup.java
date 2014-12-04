/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.router.impl;

import org.apache.ignite.lang.*;
import org.apache.ignite.lifecycle.*;
import org.gridgain.client.router.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.spring.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.net.*;
import java.util.*;
import java.util.logging.*;

import static org.gridgain.grid.kernal.GridComponentType.*;
import static org.gridgain.grid.kernal.GridProductImpl.*;

/**
 * Loader class for router.
 */
public class GridRouterCommandLineStartup {
    /** Logger. */
    @SuppressWarnings("FieldCanBeLocal")
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

            if (tcpRouter != null) {
                try {
                    tcpRouter.start();
                }
                catch (GridException e) {
                    U.error(log, "Failed to start TCP router on port " + tcpCfg.getPort() + ": " + e.getMessage(), e);

                    tcpRouter = null;
                }
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
            catch (GridException e) {
                U.error(log, "Error while stopping the router.", e);
            }
        }
    }

    /**
     * Wrapper method to run router from command-line.
     *
     * @param args Command-line arguments.
     * @throws GridException If failed.
     */
    public static void main(String[] args) throws GridException {
        X.println(
            "  _____     _     _______      _         ",
            " / ___/____(_)___/ / ___/___ _(_)___     ",
            "/ (_ // __/ // _  / (_ // _ `/ // _ \\   ",
            "\\___//_/ /_/ \\_,_/\\___/ \\_,_/_//_//_/",
            " ",
            "GridGain Router Command Line Loader",
            "ver. " + ACK_VER,
            COPYRIGHT,
            " "
        );

        GridSpringProcessor spring = SPRING.create(false);

        if (args.length < 1) {
            X.error("Missing XML configuration path.");

            System.exit(1);
        }

        String cfgPath = args[0];

        URL cfgUrl = U.resolveGridGainUrl(cfgPath);

        if (cfgUrl == null) {
            X.error("Spring XML file not found (is GRIDGAIN_HOME set?): " + cfgPath);

            System.exit(1);
        }

        boolean isLog4jUsed = U.gridClassLoader().getResource("org/apache/log4j/Appender.class") != null;

        IgniteBiTuple<Object, Object> t = null;
        Collection<Handler> savedHnds = null;

        if (isLog4jUsed)
            t = U.addLog4jNoOpLogger();
        else
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
