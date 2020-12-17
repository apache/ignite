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
import io.javalin.Javalin;
import org.apache.ignite.configuration.ConfigurationModule;
import org.apache.ignite.configuration.Configurator;
import org.apache.ignite.configuration.extended.ChangeLocal;
import org.apache.ignite.configuration.extended.Local;
import org.apache.ignite.configuration.extended.LocalConfigurationImpl;
import org.apache.ignite.configuration.extended.Selectors;
import org.apache.ignite.configuration.internal.selector.SelectorNotFoundException;
import org.apache.ignite.configuration.presentation.FormatConverter;
import org.apache.ignite.configuration.presentation.json.JsonConverter;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.slf4j.Logger;

/**
 * Rest module is responsible for starting a REST endpoints for accessing and managing configuration.
 *
 * It is started on port 8080 by default but it is possible to change this in configuration itself.
 * Refer to default config file in resources for the example.
 */
public class RestModule {
    /** */
    private static final int DFLT_PORT = 8080;

    /** */
    private static final String CONF_URL = "/management/v1/configuration/";

    /** */
    private static final String PATH_PARAM = "selector";

    /** */
    private final ConfigurationModule confModule;

    /** */
    private final Logger log;

    /** */
    public RestModule(ConfigurationModule confModule, Logger log) {
        this.confModule = confModule;
        this.log = log;
    }

    /** */
    public void start() {
        Configurator<LocalConfigurationImpl> configurator = confModule.localConfigurator();

        Integer port = configurator.getPublic(Selectors.LOCAL_REST_PORT);
        Integer portRange = configurator.getPublic(Selectors.LOCAL_REST_PORT_RANGE);
        Javalin app = null;

        if (portRange == null || portRange == 0) {
            try {
                app = Javalin.create().start(port != null ? port : DFLT_PORT);
            }
            catch (RuntimeException e) {
                log.warn("Failed to start REST endpoint: ", e);

                throw e;
            }
        }
        else {
            int startPort = port;

            for (int portCandidate = startPort; portCandidate < startPort + portRange; portCandidate++) {
                try {
                    app = Javalin.create().start(portCandidate);
                }
                catch (RuntimeException ignored) {
                    // No-op.
                }

                if (app != null)
                    break;
            }

            if (app == null) {
                String msg = "Cannot start REST endpoint. " +
                    "All ports in range [" + startPort + ", " + (startPort + portRange) + ") are in use.";

                log.warn(msg);

                throw new RuntimeException(msg);
            }
        }

        log.info("REST protocol started successfully on port " + app.port());

        FormatConverter converter = new JsonConverter();

        app.get(CONF_URL, ctx -> {
            Local local = configurator.getRoot().value();

            ctx.result(converter.convertTo("local", local));
        });

        app.get(CONF_URL + ":" + PATH_PARAM, ctx -> {
            try {
                Object subTree = configurator.getPublic(Selectors.find(ctx.pathParam(PATH_PARAM)));

                String res = converter.convertTo(subTree);

                ctx.result(res);
            }
            catch (SelectorNotFoundException selectorE) {
                ErrorResult eRes = new ErrorResult("CONFIG_PATH_UNRECOGNIZED", selectorE.getMessage());

                ctx.status(400).result(converter.convertTo("error", eRes));
            }
        });

        app.post(CONF_URL, ctx -> {
            try {
                ChangeLocal local = converter.convertFrom(ctx.body(), "local", ChangeLocal.class);

                configurator.set(Selectors.LOCAL, local);
            }
            catch (SelectorNotFoundException selectorE) {
                ErrorResult eRes = new ErrorResult("CONFIG_PATH_UNRECOGNIZED", selectorE.getMessage());

                ctx.status(400).result(converter.convertTo("error", eRes));
            }
            catch (ConfigurationValidationException validationE) {
                ErrorResult eRes = new ErrorResult("APPLICATION_EXCEPTION", validationE.getMessage());

                ctx.status(400).result(converter.convertTo("error", eRes));
            }
            catch (JsonSyntaxException e) {
                String msg = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();

                ErrorResult eRes = new ErrorResult("VALIDATION_EXCEPTION", msg);

                ctx.status(400).result(converter.convertTo("error", eRes));
            }
            catch (Exception e) {
                ErrorResult eRes = new ErrorResult("VALIDATION_EXCEPTION", e.getMessage());

                ctx.status(400).result(converter.convertTo("error", eRes));
            }
        });
    }
}
