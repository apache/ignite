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

import java.net.BindException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import com.google.gson.JsonSyntaxException;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.apache.ignite.configuration.schemas.rest.RestConfiguration;
import org.apache.ignite.configuration.schemas.rest.RestView;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.rest.netty.RestApiInitializer;
import org.apache.ignite.rest.presentation.ConfigurationPresentation;
import org.apache.ignite.rest.presentation.json.JsonPresentation;
import org.apache.ignite.rest.routes.Router;
import org.slf4j.Logger;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

/**
 * Rest module is responsible for starting a REST endpoints for accessing and managing configuration.
 *
 * It is started on port 10300 by default but it is possible to change this in configuration itself.
 * Refer to default config file in resources for the example.
 */
public class RestModule {
    /** */
    public static final int DFLT_PORT = 10300;

    /** */
    private static final String CONF_URL = "/management/v1/configuration/";

    /** */
    private static final String PATH_PARAM = "selector";

    /** */
    private ConfigurationRegistry sysConf;

    /** Ignite logger. */
    private final IgniteLogger LOG = IgniteLogger.forClass(RestModule.class);

    /** */
    private volatile ConfigurationPresentation<String> presentation;

    /** */
    private final Logger log;

    /**
     * @param log Logger.
     */
    public RestModule(Logger log) {
        this.log = log;
    }

    /**
     * @param sysCfg Configuration registry.
     */
    public void prepareStart(ConfigurationRegistry sysCfg) {
        sysConf = sysCfg;

        presentation = new JsonPresentation(sysCfg);
    }

    /**
     * @return REST channel future.
     */
    public ChannelFuture start() {
        var router = new Router();
        router
            .get(CONF_URL, (req, resp) -> {
                resp.json(presentation.represent());
            })
            .get(CONF_URL + ":" + PATH_PARAM, (req, resp) -> {
                String cfgPath = req.queryParams().get(PATH_PARAM);
                try {
                    resp.json(presentation.representByPath(cfgPath));
                }
                catch (IllegalArgumentException pathE) {
                    ErrorResult eRes = new ErrorResult("CONFIG_PATH_UNRECOGNIZED", pathE.getMessage());

                    resp.status(BAD_REQUEST);
                    resp.json(Map.of("error", eRes));
                }
            })
            .put(CONF_URL, HttpHeaderValues.APPLICATION_JSON, (req, resp) -> {
                try {
                    presentation.update(
                        req
                            .request()
                            .content()
                            .readCharSequence(req.request().content().readableBytes(), StandardCharsets.UTF_8)
                            .toString());
                }
                catch (IllegalArgumentException argE) {
                    ErrorResult eRes = new ErrorResult("CONFIG_PATH_UNRECOGNIZED", argE.getMessage());

                    resp.status(BAD_REQUEST);
                    resp.json(Map.of("error", eRes));
                }
                catch (ConfigurationValidationException validationE) {
                    ErrorResult eRes = new ErrorResult("APPLICATION_EXCEPTION", validationE.getMessage());

                    resp.status(BAD_REQUEST);
                    resp.json(Map.of("error", eRes));
                    resp.json(eRes);
                }
                catch (JsonSyntaxException e) {
                    String msg = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();

                    ErrorResult eRes = new ErrorResult("VALIDATION_EXCEPTION", msg);
                    resp.status(BAD_REQUEST);
                    resp.json(Map.of("error", eRes));
                }
                catch (Exception e) {
                    ErrorResult eRes = new ErrorResult("VALIDATION_EXCEPTION", e.getMessage());

                    resp.status(BAD_REQUEST);
                    resp.json(Map.of("error", eRes));
                }
            });

        return startRestEndpoint(router);
    }

    /** */
    private ChannelFuture startRestEndpoint(Router router) {
        RestView restConfigurationView = sysConf.getConfiguration(RestConfiguration.KEY).value();

        int desiredPort = restConfigurationView.port();
        int portRange = restConfigurationView.portRange();

        int port = 0;

        Channel ch = null;

        EventLoopGroup parentGrp = new NioEventLoopGroup();
        EventLoopGroup childGrp = new NioEventLoopGroup();

        var hnd = new RestApiInitializer(router);

        // TODO: IGNITE-15132 Rest module must reuse netty infrastructure from network module
        ServerBootstrap b = new ServerBootstrap();
        b.option(ChannelOption.SO_BACKLOG, 1024);
        b.group(parentGrp, childGrp)
            .channel(NioServerSocketChannel.class)
            .handler(new LoggingHandler(LogLevel.INFO))
            .childHandler(hnd);

        for (int portCandidate = desiredPort; portCandidate <= desiredPort + portRange; portCandidate++) {
            ChannelFuture bindRes = b.bind(portCandidate).awaitUninterruptibly();
            if (bindRes.isSuccess()) {
                ch = bindRes.channel();

                ch.closeFuture().addListener(new ChannelFutureListener() {
                    @Override public void operationComplete(ChannelFuture fut) {
                        parentGrp.shutdownGracefully();
                        childGrp.shutdownGracefully();
                        LOG.error("REST component was stopped", fut.cause());
                    }
                });
                port = portCandidate;
                break;
            }
            else if (!(bindRes.cause() instanceof BindException)) {
                parentGrp.shutdownGracefully();
                childGrp.shutdownGracefully();
                throw new RuntimeException(bindRes.cause());
            }
        }

        if (ch == null) {
            String msg = "Cannot start REST endpoint. " +
                "All ports in range [" + desiredPort + ", " + (desiredPort + portRange) + "] are in use.";

            log.error(msg);

            parentGrp.shutdownGracefully();
            childGrp.shutdownGracefully();

            throw new RuntimeException(msg);
        }

        log.info("REST protocol started successfully on port " + port);

        return ch.closeFuture();
    }
}
