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
import io.micronaut.context.ApplicationContext;
import org.apache.ignite.cli.IgniteCLIException;
import org.apache.ignite.cli.builtins.config.ConfigurationClient;
import picocli.CommandLine;

@CommandLine.Command(
    name = "config",
    description = "Inspect and update Ignite cluster configuration.",
    subcommands = {
        ConfigCommandSpec.GetConfigCommandSpec.class,
        ConfigCommandSpec.SetConfigCommandSpec.class
    }
)
public class ConfigCommandSpec extends AbstractCommandSpec {

    @Override public void run() {
        spec.commandLine().usage(spec.commandLine().getOut());
    }

    @CommandLine.Command(name = "get", description = "Get current Ignite cluster configuration values.")
    public static class GetConfigCommandSpec extends AbstractCommandSpec {

        @Inject private ConfigurationClient configurationClient;

        @CommandLine.Mixin CfgHostnameOptions cfgHostnameOptions;

        @CommandLine.Option(names = {"--subtree"},
            description = "any text representation of hocon for querying considered subtree of config " +
                "(example: local.baseline)")
        private String subtree;

        @Override public void run() {
            spec.commandLine().getOut().println(
                configurationClient.get(cfgHostnameOptions.host(), cfgHostnameOptions.port(), subtree));
        }
    }

    @CommandLine.Command(
        name = "set",
        description = "Update Ignite cluster configuration values."
    )
    public static class SetConfigCommandSpec extends AbstractCommandSpec {

        @Inject private ConfigurationClient configurationClient;

        @CommandLine.Parameters(paramLabel = "hocon-string", description = "any text representation of hocon config")
        private String config;

        @CommandLine.Mixin CfgHostnameOptions cfgHostnameOptions;

        @Override public void run() {
            spec.commandLine().getOut().println(
                configurationClient
                    .set(cfgHostnameOptions.host(), cfgHostnameOptions.port(), config));
        }
    }

    private static class CfgHostnameOptions {

        @CommandLine.Option(names = "--node-endpoint", required = true,
            description = "host:port of node for configuration")
        String cfgHostPort;


        int port() {
            var hostPort = parse();

            try {
                return Integer.parseInt(hostPort[1]);
            } catch (NumberFormatException ex) {
                throw new IgniteCLIException("Can't parse port from " + hostPort[1] + " value");
            }
        }

        String host() {
            return parse()[0];
        }

        private String[] parse() {
            var hostPort = cfgHostPort.split(":");
            if (hostPort.length != 2)
                throw new IgniteCLIException("Incorrect host:port pair provided " +
                    "(example of valid value 'localhost:8080')");
           return hostPort;
        }
    }
}
