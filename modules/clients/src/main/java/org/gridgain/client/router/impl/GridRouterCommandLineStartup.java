/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.router.impl;

import org.gridgain.client.router.*;
import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;
import org.springframework.beans.*;
import org.springframework.beans.factory.*;
import org.springframework.beans.factory.xml.*;
import org.springframework.context.*;
import org.springframework.context.support.*;
import org.springframework.core.io.*;

import java.net.*;
import java.text.*;
import java.util.*;

import static org.gridgain.grid.kernal.GridProductImpl.*;

/**
 * Loader class for router.
 */
public class GridRouterCommandLineStartup {
    /** Logger. */
    @SuppressWarnings("FieldCanBeLocal")
    private GridLogger log;

    /** TCP router. */
    private GridTcpRouterImpl tcpRouter;

    /** HTTP router. */
    private GridHttpRouterImpl httpRouter;

    /**
     * Search given context for required configuration and starts router.
     *
     * @param ctx Context to extract configuration from.
     */
    public void start(ListableBeanFactory ctx) {
        log = getBean(ctx, GridLogger.class);

        if (log == null) {
            U.error(log, "Failed to find logger definition in application context. Stopping the router.");

            return;
        }

        GridTcpRouterConfiguration tcpCfg = getBean(ctx, GridTcpRouterConfiguration.class);

        if (tcpCfg == null)
            U.warn(log, "TCP router startup skipped (configuration not found).");
        else {
            tcpRouter = new GridTcpRouterImpl(tcpCfg);

            try {
                tcpRouter.start();
            }
            catch (GridException e) {
                U.error(log, "Failed to start TCP router on port " + tcpCfg.getPort() + ": " + e.getMessage(), e);

                tcpRouter = null;
            }
        }

        GridHttpRouterConfiguration httpCfg = getBean(ctx, GridHttpRouterConfiguration.class);

        if (httpCfg == null)
            U.warn(log, "HTTP router startup skipped (configuration not found).");
        else {
            httpRouter = new GridHttpRouterImpl(httpCfg);

            try {
                httpRouter.start();
            }
            catch (GridException e) {
                U.error(log, "Failed to start HTTP router. See log above for details.", e);

                httpRouter = null;
            }
        }
    }

    /**
     * Stops router.
     */
    public void stop() {
        if (tcpRouter != null)
            tcpRouter.stop();

        if (httpRouter != null)
            httpRouter.stop();
    }

    /**
     * Wrapper method to run router from command-line.
     *
     * @param args Command-line arguments.
     * @throws GridException If failed.
     */
    public static void main(String[] args) throws GridException {
        String buildDate = new SimpleDateFormat("yyyyMMdd").format(new Date(BUILD_TSTAMP * 1000));

        String rev = REV_HASH.length() > 8 ? REV_HASH.substring(0, 8) : REV_HASH;
        String ver = "ver. " + VER + '#' + buildDate + "-sha1:" + rev;

        X.println(
            "  _____     _     _______      _         ",
            " / ___/____(_)___/ / ___/___ _(_)___     ",
            "/ (_ // __/ // _  / (_ // _ `/ // _ \\   ",
            "\\___//_/ /_/ \\_,_/\\___/ \\_,_/_//_//_/",
            " ",
            "GridGain Router Command Line Loader",
            ver,
            COPYRIGHT,
            " "
        );

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

        GridBiTuple<Object, Object> t = null;

        if (isLog4jUsed)
            t = U.addLog4jNoOpLogger();

        ApplicationContext ctx = null;

        try {
            ctx = loadCfg(cfgUrl);
        }
        finally {
            if (isLog4jUsed && t != null)
                U.removeLog4jNoOpLogger(t);
        }

        final GridRouterCommandLineStartup routerLdr = new GridRouterCommandLineStartup();

        if (ctx != null) {
            routerLdr.start(ctx);

            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override public void run() {
                    routerLdr.stop();
                }
            });
        }
        else {
            X.error("Failed to start router with given configuration. Url: ", cfgUrl);

            System.exit(1);
        }
    }

    /**
     * Reads spring context from the given location.
     * @param springCfgUrl Context descriptor loxcation.
     * @return Spring context.
     * @throws GridException If context can't be loaded.
     */
    public static ApplicationContext loadCfg(URL springCfgUrl) throws GridException {
        GenericApplicationContext springCtx;

        try {
            springCtx = new GenericApplicationContext();

            new XmlBeanDefinitionReader(springCtx).loadBeanDefinitions(new UrlResource(springCfgUrl));

            springCtx.refresh();
        }
        catch (BeansException e) {
            throw new GridException("Failed to instantiate Spring XML application context [springUrl=" +
                springCfgUrl + ", err=" + e.getMessage() + ']', e);
        }

        return springCtx;
    }

    /**
     * Get bean configuration.
     *
     * @param ctx Spring context.
     * @param beanCls Bean class.
     * @return Spring bean.
     */
    @Nullable private static <T> T getBean(ListableBeanFactory ctx, Class<T> beanCls) {
        Map.Entry<String, T> entry = F.firstEntry(ctx.getBeansOfType(beanCls));

        return entry == null ? null : entry.getValue();
    }
}
