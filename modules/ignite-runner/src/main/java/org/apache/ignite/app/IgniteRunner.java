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

package org.apache.ignite.app;

import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import org.apache.ignite.configuration.ConfigurationModule;
import org.apache.ignite.rest.RestModule;
import org.apache.ignite.utils.IgniteProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sample application integrating new configuration module and providing standard REST API to access and modify it.
 */
public class IgniteRunner {
    /** */
    private static final String CONF_PARAM_NAME = "--config";

    /** */
    private static final String DFLT_CONF_FILE_NAME = "bootstrap-config.json";

    /** */
    private static final String VER_KEY = "version";

    /** */
    private final static Logger log = LoggerFactory.getLogger(IgniteRunner.class);

    /**
     * It is possible to start application with a custom configuration in form of json file other than that in resources.
     *
     * To make application pick up custom configuration file its full path should be passed to arguments after key "--config".
     *
     * @param args Empty or providing path to custom configuration file after marker parameter "--config".
     */
    public static void main(String[] args) throws IOException {
        ackVersion();

        ConfigurationModule confModule = new ConfigurationModule();

        Reader confReader = null;

        try {
            if (args != null) {
                for (int i = 0; i < args.length; i++) {
                    if (CONF_PARAM_NAME.equals(args[i]) && i + 1 < args.length) {
                        confReader = new FileReader(args[i + 1]);

                        break;
                    }
                }
            }

            if (confReader == null) {
                confReader = new InputStreamReader(
                    IgniteRunner.class.getClassLoader().getResourceAsStream(DFLT_CONF_FILE_NAME));
            }

            confModule.bootstrap(confReader);
        }
        finally {
            if (confReader != null)
                confReader.close();
        }

        RestModule rest = new RestModule(confModule, log);

        rest.start();

        ackSuccessStart();
    }

    /** */
    private static void ackSuccessStart() {
        log.info("Ignite application started successfully");
    }

    /** */
    private static void ackVersion() {
        String ver = IgniteProperties.get(VER_KEY);

        log.info("Starting Ignite of version " + ver);
    }
}
