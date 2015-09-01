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

package org.apache.ignite.spi.deployment.uri.scanners.http;

import java.util.Collections;
import java.util.List;
import javax.servlet.http.HttpServletResponse;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.deployment.uri.GridUriDeploymentAbstractSelfTest;
import org.apache.ignite.spi.deployment.uri.UriDeploymentSpi;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTestConfig;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.util.resource.Resource;

import static org.eclipse.jetty.http.HttpHeader.LAST_MODIFIED;

/**
 * Test http scanner.
 */
@GridSpiTest(spi = UriDeploymentSpi.class, group = "Deployment SPI")
public class GridHttpDeploymentSelfTest extends GridUriDeploymentAbstractSelfTest {
    /** Jetty. */
    private Server srv;

    /** {@inheritDoc} */
    @Override protected void beforeSpiStarted() throws Exception {
        srv = new Server();

        ServerConnector conn = new ServerConnector(srv);

        conn.setPort(8080);

        srv.addConnector(conn);

        ResourceHandler hnd = new ResourceHandler() {
            @Override protected void doResponseHeaders(HttpServletResponse resp, Resource res, String mimeTyp) {
                super.doResponseHeaders(resp, res, mimeTyp);

                resp.setDateHeader(LAST_MODIFIED.asString(), res.lastModified());
            }
        };

        hnd.setDirectoriesListed(true);
        hnd.setResourceBase(
            U.resolveIgnitePath(GridTestProperties.getProperty("ant.urideployment.gar.path")).getPath());

        srv.setHandler(hnd);

        srv.start();

        assert srv.isStarted();
    }

    /**
     * @throws Exception If failed.
     */
    @Override protected void afterTestsStopped() throws Exception {
        assert srv.isStarted();

        srv.stop();

        assert srv.isStopped();
    }

    /**
     * @throws Exception if failed.
     */
    public void testDeployment() throws Exception {
        checkTask("org.apache.ignite.spi.deployment.uri.tasks.GridUriDeploymentTestTask3");
        checkTask("GridUriDeploymentTestWithNameTask3");
    }

    /**
     * @return Test server URl as deployment source URI.
     */
    @GridSpiTestConfig
    public List<String> getUriList() {
        return Collections.singletonList("http://freq=5000@localhost:8080/");
    }
}