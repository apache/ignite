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

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.internal.processors.resource.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import javax.servlet.*;
import javax.servlet.http.*;
import java.net.*;
import java.util.*;

/**
 * This class defines servlet-based GridGain startup. This startup can be used to start GridGain
 * inside any web container as servlet.
 * <p>
 * This startup must be defined in {@code web.xml} file.
 * <pre name="code" class="xml">
 * &lt;servlet&gt;
 *     &lt;servlet-name&gt;GridGain&lt;/servlet-name&gt;
 *     &lt;servlet-class&gt;org.apache.ignite.startup.servlet.GridServletStartup&lt;/servlet-class&gt;
 *     &lt;init-param&gt;
 *         &lt;param-name&gt;cfgFilePath&lt;/param-name&gt;
 *         &lt;param-value&gt;config/default-config.xml&lt;/param-value&gt;
 *     &lt;/init-param&gt;
 *     &lt;load-on-startup&gt;1&lt;/load-on-startup&gt;
 * &lt;/servlet&gt;
 * </pre>
 * <p>
 * Servlet-based startup may be used in any web container like Tomcat, Jetty and etc.
 * Depending on the way this startup is deployed the GridGain instance can be accessed
 * by either all web applications or by only one. See web container class loading architecture:
 * <ul>
 * <li><a target=_blank href="http://tomcat.apache.org/tomcat-7.0-doc/class-loader-howto.html">http://tomcat.apache.org/tomcat-7.0-doc/class-loader-howto.html</a></li>
 * <li><a target=_blank href="http://docs.codehaus.org/display/JETTY/Classloading">http://docs.codehaus.org/display/JETTY/Classloading</a></li>
 * </ul>
 * <p>
 * <h2 class="header">Tomcat</h2>
 * There are two ways to start GridGain on Tomcat.
 * <ul>
 * <li>GridGain started when web container starts and GridGain instance is accessible only to all web applications.
 * <ol>
 *     <li>Add GridGain libraries in Tomcat common loader.
 *         Add in file {@code $TOMCAT_HOME/conf/catalina.properties} for property {@code shared.loader}
 *         the following {@code $GRIDGAIN_HOME/gridgain.jar,$GRIDGAIN_HOME/libs/*.jar}
 *         (replace {@code $GRIDGAIN_HOME} with absolute path).
 *     </li>
 *     <li>Configure startup in {@code $TOMCAT_HOME/conf/web.xml}
 *         <pre name="code" class="xml">
 *         &lt;servlet&gt;
 *             &lt;servlet-name&gt;GridGain&lt;/servlet-name&gt;
 *             &lt;servlet-class&gt;org.apache.ignite.startup.servlet.GridServletStartup&lt;/servlet-class&gt;
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
 * GridGain started from WAR-file and GridGain instance is accessible only to that web application.
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
 * ServletHolder servlet = ctx.addServlet("GridGain", "/GridGainStartup",
 *      "org.apache.ignite.startup.servlet.GridServletStartup");
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
public class GridServletStartup extends HttpServlet {
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
        // Avoid multiple servlet instances. GridGain should be loaded once.
        if (loaded)
            return;

        String cfgFile = getServletConfig().getInitParameter(cfgFilePathParam);

        if (cfgFile == null)
            throw new ServletException("Failed to read property: " + cfgFilePathParam);

        URL cfgUrl = U.resolveGridGainUrl(cfgFile);

        if (cfgUrl == null)
            throw new ServletException("Failed to find Spring configuration file (path provided should be " +
                "either absolute, relative to GRIDGAIN_HOME, or relative to META-INF folder): " + cfgFile);

        try {
            IgniteBiTuple<Collection<IgniteConfiguration>, ? extends GridSpringResourceContext> t =
                GridGainEx.loadConfigurations(cfgUrl);

            Collection<IgniteConfiguration> cfgs = t.get1();

            if (cfgs == null)
                throw new ServletException("Failed to find a single grid factory configuration in: " + cfgUrl);

            for (IgniteConfiguration cfg : cfgs) {
                assert cfg != null;

                IgniteConfiguration adapter = new IgniteConfiguration(cfg);

                Ignite ignite = GridGainEx.start(adapter, t.get2());

                // Test if grid is not null - started properly.
                if (ignite != null)
                    gridNames.add(ignite.name());
            }
        }
        catch (IgniteCheckedException e) {
            // Stop started grids only.
            for (String name: gridNames)
                G.stop(name, true);

            throw new ServletException("Failed to start GridGain.", e);
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
        return S.toString(GridServletStartup.class, this);
    }
}
