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

import static picocli.CommandLine.Model.CommandSpec;
import static picocli.CommandLine.Model.OptionSpec;
import static picocli.CommandLine.Model.PositionalParamSpec;

import java.nio.file.Path;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.app.IgnitionImpl;
import picocli.CommandLine;

/**
 * The main entry point for run new Ignite node from CLI toolchain.
 */
public class IgniteCliRunner {
    /**
     * Main method for running a new Ignite node.
     *
     * <p>Usage: IgniteCliRunner [--config=configPath] --work-dir=workDir nodeName
     *
     * @param args CLI args to start a new node.
     */
    public static void main(String[] args) {
        try {
            start(args);
        } catch (CommandLine.ParameterException e) {
            System.out.println(e.getMessage());

            e.getCommandLine().usage(System.out);

            System.exit(1);
        }
    }

    /**
     * Starts a new Ignite node.
     *
     * @param args CLI args to start a new node.
     * @return New Ignite node.
     */
    public static Ignite start(String[] args) {
        CommandSpec spec = CommandSpec.create();

        spec.addOption(
                OptionSpec
                        .builder("--config")
                        .paramLabel("configPath")
                        .type(Path.class)
                        .description("Path to node configuration file in HOCON format.")
                        .build()
        );

        spec.addOption(
                OptionSpec
                        .builder("--work-dir")
                        .paramLabel("workDir")
                        .type(Path.class)
                        .description("Path to node working directory.")
                        .required(true)
                        .build()
        );

        spec.addPositional(
                PositionalParamSpec
                        .builder()
                        .paramLabel("nodeName")
                        .type(String.class)
                        .description("Node name.")
                        .required(true)
                        .build()
        );

        var cmd = new CommandLine(spec);

        var pr = cmd.parseArgs(args);

        var parsedArgs = new Args(
                pr.matchedPositionalValue(0, null),
                pr.matchedOptionValue("--config", null),
                pr.matchedOptionValue("--work-dir", null)
        );

        var ignition = new IgnitionImpl();

        if (parsedArgs.config != null) {
            return ignition.start(parsedArgs.nodeName, parsedArgs.config.toAbsolutePath(), parsedArgs.nodeWorkDir);
        } else {
            return ignition.start(parsedArgs.nodeName, parsedArgs.nodeWorkDir);
        }
    }

    /**
     * Simple value object with parsed CLI args of ignite runner.
     */
    private static class Args {
        /** Name of the node. */
        private final String nodeName;

        /** Path to config file. */
        private final Path config;

        /** Path to node work directory. */
        private final Path nodeWorkDir;

        /**
         * Creates new instance with parsed arguments.
         *
         * @param nodeName Name of the node.
         * @param config   Path to config file.
         */
        private Args(String nodeName, Path config, Path nodeWorkDir) {
            this.nodeName = nodeName;
            this.config = config;
            this.nodeWorkDir = nodeWorkDir;
        }
    }
}
