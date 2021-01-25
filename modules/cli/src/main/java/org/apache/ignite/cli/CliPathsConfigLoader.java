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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Properties;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.ignite.cli.builtins.SystemPathResolver;

/**
 * Due to the nature of Ignite CLI tool it can be runned in the environment without ignitecfg file at all.
 * This class created to simplify the managing of the different cases:
 * <ul>
 *     <li>When user download binary and run it to manage any existence remote cluster</li>
 *     <li>When user download binary and run 'init' to deploy Ignite distribution on the current machine</li>
 *     <li>When user install it by system package</li>
 * </ul>
 */
@Singleton
public class CliPathsConfigLoader {
    /** System paths' resolver. */
    private final SystemPathResolver pathRslvr;

    /** Ignite CLI tool version. */
    private final String ver;

    /**
     * Creates new loader.
     *
     * @param pathRslvr System paths' resolver.
     * @param cliVerInfo CLI version info provider.
     */
    @Inject
    public CliPathsConfigLoader(
        SystemPathResolver pathRslvr,
        CliVersionInfo cliVerInfo
    ) {
        this.pathRslvr = pathRslvr;
        this.ver = cliVerInfo.ver;
    }

    /**
     * Loads Ignite paths config from file if exists.
     *
     * @return IgnitePaths if config file exists, empty otherwise.
     */
    public Optional<IgnitePaths> loadIgnitePathsConfig() {
        if (configFilePath().toFile().exists())
            return Optional.of(readConfigFile(configFilePath(), ver));

        return Optional.empty();
    }

    /**
     * Loads Ignite paths configuration if config file exists or failed otherwise.
     *
     * @return IgnitePaths or throw exception, if no config file exists.
     */
    public IgnitePaths loadIgnitePathsOrThrowError() {
        Optional<IgnitePaths> ignitePaths = loadIgnitePathsConfig();

        if (ignitePaths.isPresent()) {
            if (!ignitePaths.get().validateDirs()) {
                throw new IgniteCLIException("Some required directories are absent. " +
                    "Try to run 'init' command to fix the issue.");
            }

            return ignitePaths.get();
        }
        else
            throw new IgniteCLIException("To execute node module/node management commands you must run 'init' first");
    }

    /**
     * @return Path to Ignite CLI config file.
     */
    public Path configFilePath() {
        return pathRslvr.osHomeDirectoryPath().resolve(".ignitecfg");
    }

    /**
     * Reads Ignite CLI configuration file and prepare {@link IgnitePaths} instance.
     *
     * @param cfgPath Path to config file.
     * @param ver Ignite CLI version.
     * @return IgnitePaths with resolved directories of current Ignite distribution.
     */
    private static IgnitePaths readConfigFile(Path cfgPath, String ver) {
        try (InputStream inputStream = new FileInputStream(cfgPath.toFile())) {
            Properties props = new Properties();
            props.load(inputStream);

            if ((props.getProperty("bin") == null) || (props.getProperty("work") == null))
                throw new IgniteCLIException("Config file has wrong format. " +
                    "It must contain correct paths to bin and work dirs");

            return new IgnitePaths(
                Path.of(props.getProperty("bin")),
                Path.of(props.getProperty("work")),
                ver);
        }
        catch (IOException e) {
            throw new IgniteCLIException("Can't read config file");
        }
    }
}
