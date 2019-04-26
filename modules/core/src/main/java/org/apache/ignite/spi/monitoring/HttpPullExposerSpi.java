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

package org.apache.ignite.spi.monitoring;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.monitoring.GridMonitoringManager;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class HttpPullExposerSpi extends IgniteSpiAdapter implements MonitoringExposerSpi {
    /** HTTP server. */
    private Server httpSrv;

    /** Monitoring manager. */
    private GridMonitoringManager mgr;

    /** Logger. */
    @LoggerResource
    protected IgniteLogger log;

    @Override public void spiStart(@Nullable String igniteInstanceName) throws IgniteSpiException {
        httpSrv = new Server(new QueuedThreadPool(200, 2));

        ServerConnector srvConn = new ServerConnector(httpSrv, new HttpConnectionFactory(new HttpConfiguration()));

        srvConn.setHost("localhost");
        srvConn.setPort(8080);
        srvConn.setIdleTimeout(30_000L);
        srvConn.setReuseAddress(true);

        httpSrv.addConnector(srvConn);

        httpSrv.setStopAtShutdown(false);

        httpSrv.setHandler(new AbstractHandler() {
            @Override public void handle(String target, Request baseRequest, HttpServletRequest request,
                HttpServletResponse response) throws IOException, ServletException {
                HttpPullExposerSpi.this.handle(target, baseRequest, request, response);
            }
        });

        try {
            httpSrv.start();
        }
        catch (Exception e) {
            throw new IgniteSpiException(e);
        }
    }

    private void handle(String target, Request req, HttpServletRequest srvReq,
        HttpServletResponse res) throws IOException, ServletException {
        if (target.startsWith("/ignite/monitoring")) {
            res.setStatus(HttpServletResponse.SC_OK);

            res.setContentType("application/json");
            res.setCharacterEncoding("UTF-8");

            Map<String, Map<?, ?>> data = new HashMap<>();

            data.put("sensors", mgr.sensors());
            data.put("lists", mgr.lists());
            data.put("sensorGroups", mgr.sensorGroups());

            ObjectMapper mapper = mapper();

            mapper.writeValue(res.getOutputStream(), data);

            res.getOutputStream();

            req.setHandled(true);
        }
        else {
            res.setStatus(HttpServletResponse.SC_NOT_FOUND);

            req.setHandled(true);
        }
    }

    @NotNull private ObjectMapper mapper() {
        ObjectMapper mapper = new ObjectMapper();

        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

        return mapper;
    }

    @Override public void spiStop() throws IgniteSpiException {

    }

    @Override public void onClientDisconnected(IgniteFuture<?> reconnectFut) {

    }

    @Override public void onClientReconnected(boolean clusterRestarted) {

    }

    @Override public String getName() {
        return "HttpPullExposerSpi";
    }

    @Override public void setMonitoringProcessor(GridMonitoringManager gridMonitoringManager) {
        this.mgr = gridMonitoringManager;
    }
}
