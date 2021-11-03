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

package org.apache.ignite.cli;

import io.micronaut.context.ApplicationContext;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import org.apache.ignite.cli.spec.IgniteCliSpec;
import org.fusesource.jansi.AnsiConsole;

/**
 * Entry point of Ignite CLI.
 */
public class IgniteCliApp {
    public static void main(String... args) {
        initJavaLoggerProps();

        ApplicationContext applicationCtx = ApplicationContext.run();

        int exitCode;

        try {
            AnsiConsole.systemInstall();

            exitCode = IgniteCliSpec.initCli(applicationCtx).execute(args);
        } finally {
            AnsiConsole.systemUninstall();
        }

        System.exit(exitCode);
    }

    /**
     * This is a temporary solution to hide unnecessary java util logs that are produced by ivy. ConsoleHandler.level should be set to
     * SEVERE.
     * TODO: https://issues.apache.org/jira/browse/IGNITE-15713
     */
    private static void initJavaLoggerProps() {
        InputStream propsFile = IgniteCliApp.class.getResourceAsStream("/cli.java.util.logging.properties");

        Path props = null;

        try {
            props = Files.createTempFile("cli.java.util.logging.properties", "");

            if (propsFile != null) {
                Files.copy(propsFile, props.toAbsolutePath(), StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException ignored) {
            // No-op.
        }

        if (props != null) {
            System.setProperty("java.util.logging.config.file", props.toString());
        }
    }
}
