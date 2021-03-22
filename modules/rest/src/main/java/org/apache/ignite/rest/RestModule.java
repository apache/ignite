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

import com.google.gson.JsonSyntaxException;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import org.apache.ignite.configuration.ConfigurationRegistry;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.rest.configuration.RestConfiguration;
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
    private static final int DFLT_PORT = 10300;

    /** */
    private static final String CONF_URL = "/management/v1/configuration/";

    /** */
    private static final String PATH_PARAM = "selector";

    /** */
    private ConfigurationRegistry sysConf;

    /** */
    private volatile ConfigurationPresentation<String> presentation;

    /** */
    private final Logger log;

    /** */
    public RestModule(Logger log) {
        this.log = log;
    }

    /** */
    public void prepareStart(ConfigurationRegistry sysCfg, Reader moduleConfReader) {
        sysConf = sysCfg;

        presentation = new JsonPresentation(Collections.emptyMap());

//        FormatConverter converter = new JsonConverter();
//
//        Configurator<RestConfigurationImpl> restConf = Configurator.create(RestConfigurationImpl::new,
//            converter.convertFrom(moduleConfReader, "rest", InitRest.class));
//
//        sysConfig.registerConfigurator(restConf);
    }

    /**
     *
     */
    public void start() throws InterruptedException {
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

        startRestEndpoint(router);
    }

    /** */
    private void startRestEndpoint(Router router) throws InterruptedException {
        Integer desiredPort = sysConf.getConfiguration(RestConfiguration.KEY).port().value();
        Integer portRange = sysConf.getConfiguration(RestConfiguration.KEY).portRange().value();

        int port = 0;

        if (portRange == null || portRange == 0) {
            try {
                port = (desiredPort != null ? desiredPort : DFLT_PORT);
            }
            catch (RuntimeException e) {
                log.warn("Failed to start REST endpoint: ", e);

                throw e;
            }
        }
        else {
            int startPort = desiredPort;

            for (int portCandidate = startPort; portCandidate < startPort + portRange; portCandidate++) {
                try {
                    port = (portCandidate);
                }
                catch (RuntimeException ignored) {
                    // No-op.
                }
            }

            if (port == 0) {
                String msg = "Cannot start REST endpoint. " +
                    "All ports in range [" + startPort + ", " + (startPort + portRange) + "] are in use.";

                log.warn(msg);

                throw new RuntimeException(msg);
            }
        }

        EventLoopGroup bossGrp = new NioEventLoopGroup(1);
        EventLoopGroup workerGrp = new NioEventLoopGroup();
        var hnd = new RestApiInitializer(router);
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.option(ChannelOption.SO_BACKLOG, 1024);
            b.group(bossGrp, workerGrp)
                .channel(NioServerSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(hnd);

            Channel ch = b.bind(port).sync().channel();

            if (log.isInfoEnabled())
                log.info("REST protocol started successfully on port " + port);

            ch.closeFuture().sync();
        }
        finally {
            bossGrp.shutdownGracefully();
            workerGrp.shutdownGracefully();
        }
    }

    /** */
    public String configRootKey() {
        return "rest";
    }
}
