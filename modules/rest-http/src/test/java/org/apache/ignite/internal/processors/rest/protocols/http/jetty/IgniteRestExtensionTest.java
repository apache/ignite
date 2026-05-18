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

package org.apache.ignite.internal.processors.rest.protocols.http.jetty;

import java.io.IOException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteVersionUtils;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.internal.processors.rest.protocols.http.jetty.RestSetupSimpleTest.execute;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;

/** */
public class IgniteRestExtensionTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConnectorConfiguration(new ConnectorConfiguration());
    }

    /** */
    @Test
    public void test() throws Exception {
        startGrid();

        assertThat(execute("/ignite", new T2<>("cmd", "version")),
            containsString(IgniteVersionUtils.VER_STR));

        assertThat(execute("/ext1/help"),
            containsString("Extension 1."));

        assertThat(execute("/ext2/help"),
            containsString("Extension 2."));
    }

    /** */
    public static class TestRestExtension1 implements IgniteRestExtension {
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public void configure(ServletContextHandler ctx) {
            ctx.setContextPath("/ext1");

            ctx.addServlet(new ServletHolder(new HttpServlet() {
                protected void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
                    res.getWriter().print("Extension 1.");
                }
            }), "/help");

            ctx.addServlet(new ServletHolder(new HttpServlet() {
                protected void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
                    try {
                        ignite.cluster().state(INACTIVE);
                    }
                    catch (SecurityException e) {
                        res.setStatus(HttpServletResponse.SC_FORBIDDEN);
                        res.getWriter().print("Authorization failed.");
                    }
                }
            }), "/deactivate");
        }
    }

    /** */
    public static class TestRestExtension2 implements IgniteRestExtension {
        /** {@inheritDoc} */
        @Override public void configure(ServletContextHandler ctx) {
            ctx.setContextPath("/ext2");

            ctx.addServlet(new ServletHolder(new HttpServlet() {
                protected void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
                    res.getWriter().print("Extension 2.");
                }
            }), "/help");
        }
    }
}
