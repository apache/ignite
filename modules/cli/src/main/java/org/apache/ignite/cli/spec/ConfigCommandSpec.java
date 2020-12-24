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

@CommandLine.Command(
    name = "config",
    description = "Inspects and updates Ignite cluster configuration.",
    subcommands = {
        ConfigCommandSpec.GetConfigCommandSpec.class,
        ConfigCommandSpec.SetConfigCommandSpec.class
    }
)
public class ConfigCommandSpec extends CategorySpec {
    @CommandLine.Command(name = "get", description = "Gets current Ignite cluster configuration values.")
    public static class GetConfigCommandSpec extends CommandSpec {

        @Inject private ConfigurationClient configurationClient;

        @CommandLine.Mixin CfgHostnameOptions cfgHostnameOptions;

        @CommandLine.Option(
            names = "--selector",
            description = "Configuration selector (example: local.baseline)"
        )
        private String selector;

        @Override public void run() {
            spec.commandLine().getOut().println(
                configurationClient.get(cfgHostnameOptions.host(), cfgHostnameOptions.port(), selector));
        }
    }

    @CommandLine.Command(
        name = "set",
        description = "Updates Ignite cluster configuration values."
    )
    public static class SetConfigCommandSpec extends CommandSpec {

        @Inject private ConfigurationClient configurationClient;

        @CommandLine.Parameters(paramLabel = "hocon", description = "Configuration in Hocon format")
        private String config;

        @CommandLine.Mixin CfgHostnameOptions cfgHostnameOptions;

        @Override public void run() {
            configurationClient.set(cfgHostnameOptions.host(), cfgHostnameOptions.port(), config,
                spec.commandLine().getOut(), spec.commandLine().getColorScheme());
        }
    }

    private static class CfgHostnameOptions {

        @CommandLine.Option(
            names = "--node-endpoint",
            description = "Ignite server node's REST API address and port number",
            paramLabel = "host:port"
        )
        String endpoint;

        int port() {
            if (endpoint == null)
                return 8080;

            var hostPort = parse();

            try {
                return Integer.parseInt(hostPort[1]);
            } catch (NumberFormatException ex) {
                throw new IgniteCLIException("Can't parse port from " + hostPort[1] + " value");
            }
        }

        String host() {
            return endpoint != null ? parse()[0] : "localhost";
        }

        private String[] parse() {
            var hostPort = endpoint.split(":");
            if (hostPort.length != 2)
                throw new IgniteCLIException("Incorrect host:port pair provided " +
                    "(example of valid value 'localhost:8080')");
           return hostPort;
        }
    }
}
