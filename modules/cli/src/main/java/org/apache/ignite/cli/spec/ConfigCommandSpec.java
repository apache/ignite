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

package org.apache.ignite.cli.spec;

import javax.inject.Inject;
import org.apache.ignite.cli.IgniteCLIException;
import org.apache.ignite.cli.builtins.config.ConfigurationClient;
import picocli.CommandLine;

/**
 * Commands get/put Ignite node configurations.
 */
@CommandLine.Command(
    name = "config",
    description = "Inspects and updates Ignite cluster configuration.",
    subcommands = {
        ConfigCommandSpec.GetConfigCommandSpec.class,
        ConfigCommandSpec.SetConfigCommandSpec.class
    }
)
public class ConfigCommandSpec extends CategorySpec {
    /**
     * Command for get Ignite node configurations.
     */
    @CommandLine.Command(name = "get", description = "Gets current Ignite cluster configuration values.")
    public static class GetConfigCommandSpec extends CommandSpec {
        /** Configuration client for REST node API. */
        @Inject
        private ConfigurationClient configurationClient;

        /** Command option for setting custom node host. */
        @CommandLine.Mixin
        private CfgHostnameOptions cfgHostnameOptions;

        /** Command option for setting HOCON based config selector. */
        @CommandLine.Option(
            names = "--selector",
            description = "Configuration selector (example: local.baseline)"
        )
        private String selector;

        /** {@inheritDoc} */
        @Override public void run() {
            spec.commandLine().getOut().println(
                configurationClient.get(cfgHostnameOptions.host(), cfgHostnameOptions.port(), selector));
        }
    }

    /**
     * Command for setting Ignite node configuration.
     */
    @CommandLine.Command(
        name = "set",
        description = "Updates Ignite cluster configuration values."
    )
    public static class SetConfigCommandSpec extends CommandSpec {
        /** Configuration client for REST node APi. */
        @Inject
        private ConfigurationClient configurationClient;

        /** Config string with valid HOCON value. */
        @CommandLine.Parameters(paramLabel = "hocon", description = "Configuration in Hocon format")
        private String cfg;

        /** Command option for setting custome node host. */
        @CommandLine.Mixin
        private CfgHostnameOptions cfgHostnameOptions;

        /** {@inheritDoc} */
        @Override public void run() {
            configurationClient.set(cfgHostnameOptions.host(), cfgHostnameOptions.port(), cfg,
                spec.commandLine().getOut(), spec.commandLine().getColorScheme());
        }
    }

    /**
     * Prepared picocli mixin for generic node hostname option.
     */
    private static class CfgHostnameOptions {
        /** Custom node REST endpoint address. */
        @CommandLine.Option(
            names = "--node-endpoint",
            description = "Ignite server node's REST API address and port number",
            paramLabel = "host:port"
        )
        private String endpoint;

        /**
         * @return REST endpoint port.
         */
        private int port() {
            if (endpoint == null)
                return 10300;

            var hostPort = parse();

            try {
                return Integer.parseInt(hostPort[1]);
            } catch (NumberFormatException ex) {
                throw new IgniteCLIException("Can't parse port from " + hostPort[1] + " value");
            }
        }

        /**
         * @return REST endpoint host.
         */
        private String host() {
            return endpoint != null ? parse()[0] : "localhost";
        }

        /**
         * Parses REST endpoint host and port from string.
         *
         * @return 2-elements array [host, port].
         */
        private String[] parse() {
            var hostPort = endpoint.split(":");

            if (hostPort.length != 2)
                throw new IgniteCLIException("Incorrect host:port pair provided " +
                    "(example of valid value 'localhost:10300')");

           return hostPort;
        }
    }
}
