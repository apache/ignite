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

package org.apache.ignite.agent;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.LogManager;

/**
 * Configurator for java.util.Logger.
 */
public class AgentLoggingConfigurator {
    /** */
    private static final String CFG_PATH_PROPERTY = "log.config.path";

    /** */
    private static final String PROPERTIES_FILE = "logging.properties";

    /**
     * Perform configure.
     */
    public static void configure() {
        try {
            if (System.getProperty(CFG_PATH_PROPERTY) != null) {
                File logCfg = new File(System.getProperty(CFG_PATH_PROPERTY));

                if (!logCfg.isFile()) {
                    System.err.println("Failed to load logging configuration, file not found: " + logCfg);

                    System.exit(1);
                }

                readConfiguration(logCfg);

                return;
            }

            File agentHome = AgentUtils.getAgentHome();

            if (agentHome != null) {
                File logCfg = new File(agentHome, PROPERTIES_FILE);

                if (logCfg.isFile()) {
                    readConfiguration(logCfg);

                    return;
                }
            }

            LogManager.getLogManager().readConfiguration(AgentLauncher.class.getResourceAsStream("/"
                + PROPERTIES_FILE));
        }
        catch (IOException e) {
            System.err.println("Failed to load logging configuration.");

            e.printStackTrace();

            System.exit(1);
        }
    }

    /**
     * @param file File.
     */
    private static void readConfiguration(File file) throws IOException {
        try (InputStream in = new BufferedInputStream(new FileInputStream(file))) {
            LogManager.getLogManager().readConfiguration(in);
        }
    }
}
