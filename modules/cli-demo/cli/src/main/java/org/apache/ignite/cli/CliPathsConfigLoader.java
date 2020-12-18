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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Properties;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.ignite.cli.builtins.SystemPathResolver;

@Singleton
public class CliPathsConfigLoader {

    private final SystemPathResolver pathResolver;
    private final String version;

    @Inject
    public CliPathsConfigLoader(SystemPathResolver pathResolver,
        CliVersionInfo cliVersionInfo) {
        this.pathResolver = pathResolver;
        this.version = cliVersionInfo.version;
    }

    public Optional<IgnitePaths> loadIgnitePathsConfig() {
        return searchConfigPathsFile()
            .map(f -> CliPathsConfigLoader.readConfigFile(f, version));
    }

    public IgnitePaths loadIgnitePathsOrThrowError() {
        Optional<IgnitePaths> ignitePaths = loadIgnitePathsConfig();
        if (ignitePaths.isPresent())
            return ignitePaths.get();
        else
            throw new IgniteCLIException("To execute node module/node management commands you must run 'init' first");
    }

    public Optional<File> searchConfigPathsFile() {
        File homeDirCfg = pathResolver.osHomeDirectoryPath().resolve(".ignitecfg").toFile();
        if (homeDirCfg.exists())
            return Optional.of(homeDirCfg);

        return Optional.empty();
    }

    private static IgnitePaths readConfigFile(File configFile, String version) {
        try (InputStream inputStream = new FileInputStream(configFile)) {
            Properties properties = new Properties();
            properties.load(inputStream);
            if ((properties.getProperty("bin") == null) || (properties.getProperty("work") == null))
                throw new IgniteCLIException("Config file has wrong format. " +
                    "It must contain correct paths to bin and work dirs");
            return new IgnitePaths(Path.of(properties.getProperty("bin")),
                Path.of(properties.getProperty("work")), version);
        }
        catch (IOException e) {
            throw new IgniteCLIException("Can't read config file");
        }
    }
}
