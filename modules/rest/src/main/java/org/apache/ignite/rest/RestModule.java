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

package org.apache.ignite.rest;

import static io.netty.handler.codec.http.HttpHeaderValues.APPLICATION_JSON;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static java.nio.charset.StandardCharsets.UTF_8;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import java.net.BindException;
import java.util.Map;
import org.apache.ignite.configuration.schemas.rest.RestConfiguration;
import org.apache.ignite.configuration.schemas.rest.RestView;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.NettyBootstrapFactory;
import org.apache.ignite.rest.netty.RestApiHttpRequest;
import org.apache.ignite.rest.netty.RestApiHttpResponse;
import org.apache.ignite.rest.netty.RestApiInitializer;
import org.apache.ignite.rest.presentation.ConfigurationPresentation;
import org.apache.ignite.rest.presentation.hocon.HoconPresentation;
import org.apache.ignite.rest.routes.Router;

/**
 * Rest module is responsible for starting a REST endpoints for accessing and managing configuration.
 *
 * <p>It is started on port 10300 by default but it is possible to change this in configuration itself. Refer to default config file in
 * resources for the example.
 */
public class RestModule implements IgniteComponent {
    /** Default port. */
    public static final int DFLT_PORT = 10300;

    /** Node configuration route. */
    private static final String NODE_CFG_URL = "/management/v1/configuration/node/";

    /** Cluster configuration route. */
    private static final String CLUSTER_CFG_URL = "/management/v1/configuration/cluster/";

    /** Path parameter. */
    private static final String PATH_PARAM = "selector";

    /** Ignite logger. */
    private final IgniteLogger log = IgniteLogger.forClass(RestModule.class);

    /** Node configuration register. */
    private final ConfigurationRegistry nodeCfgRegistry;

    /** Presentation of node configuration. */
    private final ConfigurationPresentation<String> nodeCfgPresentation;

    /** Presentation of cluster configuration. */
    private final ConfigurationPresentation<String> clusterCfgPresentation;

    /** Netty bootstrap factory. */
    private final NettyBootstrapFactory bootstrapFactory;

    /** Netty channel. */
    private volatile Channel channel;

    /**
     * Creates a new instance of REST module.
     *
     * @param nodeCfgMgr       Node configuration manager.
     * @param clusterCfgMgr    Cluster configuration manager.
     * @param bootstrapFactory Netty bootstrap factory.
     */
    public RestModule(
            ConfigurationManager nodeCfgMgr,
            ConfigurationManager clusterCfgMgr,
            NettyBootstrapFactory bootstrapFactory) {
        nodeCfgRegistry = nodeCfgMgr.configurationRegistry();

        nodeCfgPresentation = new HoconPresentation(nodeCfgMgr.configurationRegistry());
        clusterCfgPresentation = new HoconPresentation(clusterCfgMgr.configurationRegistry());

        this.bootstrapFactory = bootstrapFactory;
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        if (channel != null) {
            throw new IgniteException("RestModule is already started.");
        }

        var router = new Router();

        router
                .get(
                        NODE_CFG_URL,
                        (req, resp) -> resp.json(nodeCfgPresentation.represent())
                )
                .get(
                        CLUSTER_CFG_URL,
                        (req, resp) -> resp.json(clusterCfgPresentation.represent())
                )
                .get(
                        NODE_CFG_URL + ":" + PATH_PARAM,
                        (req, resp) -> handleRepresentByPath(req, resp, nodeCfgPresentation)
                )
                .get(
                        CLUSTER_CFG_URL + ":" + PATH_PARAM,
                        (req, resp) -> handleRepresentByPath(req, resp, clusterCfgPresentation)
                )
                .put(
                        NODE_CFG_URL,
                        APPLICATION_JSON,
                        (req, resp) -> handleUpdate(req, resp, nodeCfgPresentation)
                )
                .put(
                        CLUSTER_CFG_URL,
                        APPLICATION_JSON,
                        (req, resp) -> handleUpdate(req, resp, clusterCfgPresentation)
                );

        channel = startRestEndpoint(router).channel();
    }

    /**
     * Start endpoint.
     *
     * @param router Dispatcher of http requests.
     * @return Future which will be notified when this channel is closed.
     * @throws RuntimeException if this module cannot be bound to a port.
     */
    private ChannelFuture startRestEndpoint(Router router) {
        RestView restConfigurationView = nodeCfgRegistry.getConfiguration(RestConfiguration.KEY).value();

        int desiredPort = restConfigurationView.port();
        int portRange = restConfigurationView.portRange();

        int port = 0;

        Channel ch = null;

        var hnd = new RestApiInitializer(router);

        ServerBootstrap b = bootstrapFactory.createServerBootstrap()
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(hnd);

        for (int portCandidate = desiredPort; portCandidate <= desiredPort + portRange; portCandidate++) {
            ChannelFuture bindRes = b.bind(portCandidate).awaitUninterruptibly();

            if (bindRes.isSuccess()) {
                ch = bindRes.channel();
                port = portCandidate;
                break;
            } else if (!(bindRes.cause() instanceof BindException)) {
                throw new RuntimeException(bindRes.cause());
            }
        }

        if (ch == null) {
            String msg = "Cannot start REST endpoint. "
                    + "All ports in range [" + desiredPort + ", " + (desiredPort + portRange) + "] are in use.";

            log.error(msg);

            throw new RuntimeException(msg);
        }

        if (log.isInfoEnabled()) {
            log.info("REST protocol started successfully on port " + port);
        }

        return ch.closeFuture();
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        if (channel != null) {
            channel.close().await();

            channel = null;
        }
    }

    /**
     * Handle a request to get the configuration by {@link #PATH_PARAM path}.
     *
     * @param req          Rest request.
     * @param res          Rest response.
     * @param presentation Configuration presentation.
     */
    private void handleRepresentByPath(
            RestApiHttpRequest req,
            RestApiHttpResponse res,
            ConfigurationPresentation<String> presentation
    ) {
        try {
            String cfgPath = req.queryParams().get(PATH_PARAM);

            res.json(presentation.representByPath(cfgPath));
        } catch (IllegalArgumentException pathE) {
            ErrorResult errRes = new ErrorResult("CONFIG_PATH_UNRECOGNIZED", pathE.getMessage());

            res.status(BAD_REQUEST);
            res.json(Map.of("error", errRes));
        }
    }

    /**
     * Handle a configuration update request as json.
     *
     * @param req          Rest request.
     * @param res          Rest response.
     * @param presentation Configuration presentation.
     */
    private void handleUpdate(
            RestApiHttpRequest req,
            RestApiHttpResponse res,
            ConfigurationPresentation<String> presentation
    ) {
        try {
            String updateReq = req
                    .request()
                    .content()
                    .readCharSequence(req.request().content().readableBytes(), UTF_8)
                    .toString();

            presentation.update(updateReq);
        } catch (IllegalArgumentException e) {
            ErrorResult errRes = new ErrorResult("INVALID_CONFIG_FORMAT", e.getMessage());

            res.status(BAD_REQUEST);
            res.json(Map.of("error", errRes));
        } catch (ConfigurationValidationException e) {
            ErrorResult errRes = new ErrorResult("VALIDATION_EXCEPTION", e.getMessage());

            res.status(BAD_REQUEST);
            res.json(Map.of("error", errRes));
        } catch (IgniteException e) {
            ErrorResult errRes = new ErrorResult("APPLICATION_EXCEPTION", e.getMessage());

            res.status(BAD_REQUEST);
            res.json(Map.of("error", errRes));
        }
    }
}
