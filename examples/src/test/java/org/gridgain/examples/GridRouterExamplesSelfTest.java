/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples;

import org.gridgain.client.router.*;
import org.gridgain.examples.misc.client.router.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;
import org.springframework.beans.*;
import org.springframework.beans.factory.*;
import org.springframework.beans.factory.xml.*;
import org.springframework.context.*;
import org.springframework.context.support.*;
import org.springframework.core.io.*;

import java.net.*;
import java.util.*;

/**
 * GridRouterExample self test.
 */
public class GridRouterExamplesSelfTest extends GridAbstractExamplesTest {
    /**
     * @throws Exception If failed.
     */
    @Override protected void beforeTest() throws Exception {
        // Start up a router.
        startRouter("config/router/default-router.xml");

        // Start up a grid node.
        startGrid("grid-router-examples", "examples/config/example-cache.xml");
    }

    /**
     * @throws Exception If failed.
     */
    @Override protected void afterTest() throws Exception {
        GridRouterFactory.stopAllRouters();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testGridRouterExample() throws Exception {
        RouterExample.main(EMPTY_ARGS);
    }

    /**
     * Starts router.
     *
     * @param cfgPath Path to router config.
     * @throws GridException Thrown in case of any errors.
     */
    protected static void startRouter(String cfgPath) throws GridException {
        URL cfgUrl = U.resolveGridGainUrl(cfgPath);

        if (cfgUrl == null)
            throw new GridException("Spring XML file not found (is GRIDGAIN_HOME set?): " + cfgPath);

        ApplicationContext ctx = loadCfg(cfgUrl);

        if (ctx == null)
            throw new GridException("Application context can not be null");

        GridTcpRouterConfiguration tcpCfg = getBean(ctx, GridTcpRouterConfiguration.class);

        if (tcpCfg == null)
            throw new GridException("GridTcpRouterConfiguration is not found");

        GridRouterFactory.startTcpRouter(tcpCfg);
    }

    /**
     * Reads spring context from the given location.
     * @param springCfgUrl Context descriptor loxcation.
     * @return Spring context.
     * @throws GridException If context can't be loaded.
     */
    private static ApplicationContext loadCfg(URL springCfgUrl) throws GridException {
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
    @Nullable public static <T> T getBean(ListableBeanFactory ctx, Class<T> beanCls) {
        Map.Entry<String, T> entry = F.firstEntry(ctx.getBeansOfType(beanCls));

        return entry == null ? null : entry.getValue();
    }
}
