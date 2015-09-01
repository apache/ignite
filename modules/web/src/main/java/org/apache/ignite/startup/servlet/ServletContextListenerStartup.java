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

package org.apache.ignite.startup.servlet;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.resource.GridSpringResourceContext;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 * This class defines Ignite startup based on servlet context listener.
 * This startup can be used to start Ignite inside any web container.
 * <p>
 * This startup must be defined in {@code web.xml} file.
 * <pre name="code" class="xml">
 * &lt;listener&gt;
 *     &lt;listener-class&gt;org.apache.ignite.startup.servlet.ServletContextListenerStartup&lt;/listener-class&gt;
 * &lt;/listener&gt;
 *
 * &lt;context-param&gt;
 *     &lt;param-name&gt;IgniteConfigurationFilePath&lt;/param-name&gt;
 *     &lt;param-value&gt;config/default-config.xml&lt;/param-value&gt;
 * &lt;/context-param&gt;
 * </pre>
 * <p>
 * Servlet context listener based startup may be used in any web container like Tomcat, Jetty and etc.
 * Depending on the way this startup is deployed the Ignite instance can be accessed
 * by either all web applications or by only one. See web container class loading architecture:
 * <ul>
 * <li><a target=_blank href="http://tomcat.apache.org/tomcat-7.0-doc/class-loader-howto.html">http://tomcat.apache.org/tomcat-7.0-doc/class-loader-howto.html</a></li>
 * <li><a target=_blank href="http://docs.codehaus.org/display/JETTY/Classloading">http://docs.codehaus.org/display/JETTY/Classloading</a></li>
 * </ul>
 * <p>
 * <h2 class="header">Tomcat</h2>
 * There are two ways to start Ignite on Tomcat.
 * <ul>
 * <li>Ignite started when web container starts and Ignite instance is accessible only to all web applications.
 *     <ol>
 *     <li>Add Ignite libraries in Tomcat common loader.
 *         Add in file {@code $TOMCAT_HOME/conf/catalina.properties} for property {@code common.loader}
 *         the following {@code $IGNITE_HOME/*.jar,$IGNITE_HOME/libs/*.jar}
 *         (replace {@code $IGNITE_HOME} with absolute path).
 *     </li>
 *     <li>Configure this startup in {@code $TOMCAT_HOME/conf/web.xml}
 *         <pre name="code" class="xml">
 *         &lt;listener&gt;
 *             &lt;listener-class&gt;org.apache.ignite.startup.servlet.ServletContextListenerStartup&lt;/listener-class&gt;
 *         &lt;/listener&gt;
 *
 *         &lt;context-param&gt;
 *             &lt;param-name&gt;IgniteConfigurationFilePath&lt;/param-name&gt;
 *             &lt;param-value&gt;config/default-config.xml&lt;/param-value&gt;
 *         &lt;/context-param&gt;
 *         </pre>
 *     </li>
 *     </ol>
 * </li>
 * <li>
 * Ignite started from WAR-file and Ignite instance is accessible only to that web application.
 * Difference with approach described above is that {@code web.xml} file and all libraries should
 * be added in WAR file without changes in Tomcat configuration files.
 * </li>
 * </ul>
 */
public class ServletContextListenerStartup implements ServletContextListener {
    /** Configuration file path parameter name. */
    public static final String IGNITE_CFG_FILE_PATH_PARAM = "IgniteConfigurationFilePath";

    /** Names of started grids. */
    private final Collection<String> gridNames = new ArrayList<>();

    /** {@inheritDoc} */
    @Override public void contextInitialized(ServletContextEvent evt) {
        ServletContext ctx = evt.getServletContext();

        String cfgFile = ctx.getInitParameter(IGNITE_CFG_FILE_PATH_PARAM);

        Collection<IgniteConfiguration> cfgs;
        GridSpringResourceContext rsrcCtx = null;

        if (cfgFile != null) {
            URL cfgUrl = null;

            try {
                cfgUrl = evt.getServletContext().getResource("/META-INF/" + cfgFile);
            }
            catch (MalformedURLException ignored) {
                // Ignore, we still need to try with IGNITE_HOME.
            }

            if (cfgUrl == null)
                // Try with IGNITE_HOME and with context class loader.
                cfgUrl = U.resolveIgniteUrl(cfgFile);

            if (cfgUrl == null)
                throw new IgniteException("Failed to find Spring configuration file (path provided should be " +
                    "either absolute, relative to IGNITE_HOME, or relative to META-INF folder): " + cfgFile);

            IgniteBiTuple<Collection<IgniteConfiguration>, ? extends GridSpringResourceContext> t;

            try {
                t = IgnitionEx.loadConfigurations(cfgUrl);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to load Ignite configuration.", e);
            }

            cfgs = t.get1();
            rsrcCtx  = t.get2();

            if (cfgs.isEmpty())
                throw new IgniteException("Can't find grid factory configuration in: " + cfgUrl);
        }
        else
            cfgs = Collections.<IgniteConfiguration>singleton(new IgniteConfiguration());

        try {
            assert !cfgs.isEmpty();

            for (IgniteConfiguration cfg : cfgs) {
                assert cfg != null;

                Ignite ignite;

                synchronized (ServletContextListenerStartup.class) {
                    try {
                        ignite = G.ignite(cfg.getGridName());
                    }
                    catch (IgniteIllegalStateException ignored) {
                        ignite = IgnitionEx.start(new IgniteConfiguration(cfg), rsrcCtx);
                    }
                }

                // Check if grid is not null - started properly.
                if (ignite != null)
                    gridNames.add(ignite.name());
            }
        }
        catch (IgniteCheckedException e) {
            // Stop started grids only.
            for (String name : gridNames)
                G.stop(name, true);

            throw new IgniteException("Failed to start Ignite.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void contextDestroyed(ServletContextEvent evt) {
        // Stop started grids only.
        for (String name: gridNames)
            G.stop(name, true);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ServletContextListenerStartup.class, this);
    }
}