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

import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.resource.GridSpringResourceContext;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 * This class defines servlet-based Ignite startup. This startup can be used to start Ignite
 * inside any web container as servlet.
 * <p>
 * This startup must be defined in {@code web.xml} file.
 * <pre name="code" class="xml">
 * &lt;servlet&gt;
 *     &lt;servlet-name&gt;Ignite&lt;/servlet-name&gt;
 *     &lt;servlet-class&gt;org.apache.ignite.startup.servlet.ServletStartup&lt;/servlet-class&gt;
 *     &lt;init-param&gt;
 *         &lt;param-name&gt;cfgFilePath&lt;/param-name&gt;
 *         &lt;param-value&gt;config/default-config.xml&lt;/param-value&gt;
 *     &lt;/init-param&gt;
 *     &lt;load-on-startup&gt;1&lt;/load-on-startup&gt;
 * &lt;/servlet&gt;
 * </pre>
 * <p>
 * Servlet-based startup may be used in any web container like Tomcat, Jetty and etc.
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
 * <ol>
 *     <li>Add Ignite libraries in Tomcat common loader.
 *         Add in file {@code $TOMCAT_HOME/conf/catalina.properties} for property {@code shared.loader}
 *         the following {@code $IGNITE_HOME/ignite.jar,$IGNITE_HOME/libs/*.jar}
 *         (replace {@code $IGNITE_HOME} with absolute path).
 *     </li>
 *     <li>Configure startup in {@code $TOMCAT_HOME/conf/web.xml}
 *         <pre name="code" class="xml">
 *         &lt;servlet&gt;
 *             &lt;servlet-name&gt;Ignite&lt;/servlet-name&gt;
 *             &lt;servlet-class&gt;org.apache.ignite.startup.servlet.ServletStartup&lt;/servlet-class&gt;
 *             &lt;init-param&gt;
 *                 &lt;param-name&gt;cfgFilePath&lt;/param-name&gt;
 *                 &lt;param-value&gt;config/default-config.xml&lt;/param-value&gt;
 *             &lt;/init-param&gt;
 *             &lt;load-on-startup&gt;1&lt;/load-on-startup&gt;
 *         &lt;/servlet&gt;
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
 * <p>
 * <h2 class="header">Jetty</h2>
 * Below is Java code example with Jetty API:
 * <pre name="code" class="java">
 * Server service = new Server();
 *
 * service.addListener("localhost:8090");
 *
 * ServletHttpContext ctx = (ServletHttpContext)service.getContext("/");
 *
 * ServletHolder servlet = ctx.addServlet("Ignite", "/IgniteStartup",
 *      "org.apache.ignite.startup.servlet.ServletStartup");
 *
 * servlet.setInitParameter("cfgFilePath", "config/default-config.xml");
 *
 * servlet.setInitOrder(1);
 *
 * servlet.start();
 *
 * service.start();
 * </pre>
 */
public class ServletStartup extends HttpServlet {
    /** */
    private static final long serialVersionUID = 0L;

    /** Grid loaded flag. */
    private static boolean loaded;

    /** Configuration file path variable name. */
    private static final String cfgFilePathParam = "cfgFilePath";

    /** */
    private Collection<String> gridNames = new ArrayList<>();

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public void init() throws ServletException {
        // Avoid multiple servlet instances. Ignite should be loaded once.
        if (loaded)
            return;

        String cfgFile = getServletConfig().getInitParameter(cfgFilePathParam);

        if (cfgFile == null)
            throw new ServletException("Failed to read property: " + cfgFilePathParam);

        URL cfgUrl = U.resolveIgniteUrl(cfgFile);

        if (cfgUrl == null)
            throw new ServletException("Failed to find Spring configuration file (path provided should be " +
                "either absolute, relative to IGNITE_HOME, or relative to META-INF folder): " + cfgFile);

        try {
            IgniteBiTuple<Collection<IgniteConfiguration>, ? extends GridSpringResourceContext> t =
                IgnitionEx.loadConfigurations(cfgUrl);

            Collection<IgniteConfiguration> cfgs = t.get1();

            if (cfgs == null)
                throw new ServletException("Failed to find a single grid factory configuration in: " + cfgUrl);

            for (IgniteConfiguration cfg : cfgs) {
                assert cfg != null;

                IgniteConfiguration adapter = new IgniteConfiguration(cfg);

                Ignite ignite = IgnitionEx.start(adapter, t.get2());

                // Test if grid is not null - started properly.
                if (ignite != null)
                    gridNames.add(ignite.name());
            }
        }
        catch (IgniteCheckedException e) {
            // Stop started grids only.
            for (String name: gridNames)
                G.stop(name, true);

            throw new ServletException("Failed to start Ignite.", e);
        }

        loaded = true;
    }

    /** {@inheritDoc} */
    @Override public void destroy() {
        // Stop started grids only.
        for (String name: gridNames)
            G.stop(name, true);

        loaded = false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ServletStartup.class, this);
    }
}