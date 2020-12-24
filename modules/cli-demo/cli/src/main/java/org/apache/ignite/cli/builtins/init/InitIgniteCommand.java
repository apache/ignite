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

package org.apache.ignite.cli.builtins.init;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import javax.inject.Inject;
import org.apache.ignite.cli.CliPathsConfigLoader;
import org.apache.ignite.cli.CliVersionInfo;
import org.apache.ignite.cli.IgniteCLIException;
import org.apache.ignite.cli.IgnitePaths;
import org.apache.ignite.cli.Table;
import org.apache.ignite.cli.builtins.SystemPathResolver;
import org.apache.ignite.cli.builtins.module.ModuleManager;
import org.jetbrains.annotations.NotNull;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Help.ColorScheme;

public class InitIgniteCommand {

    private final SystemPathResolver pathResolver;
    private final CliVersionInfo cliVersionInfo;
    private final ModuleManager moduleManager;
    private final CliPathsConfigLoader cliPathsConfigLoader;

    @Inject
    public InitIgniteCommand(SystemPathResolver pathResolver, CliVersionInfo cliVersionInfo,
        ModuleManager moduleManager, CliPathsConfigLoader cliPathsConfigLoader) {
        this.pathResolver = pathResolver;
        this.cliVersionInfo = cliVersionInfo;
        this.moduleManager = moduleManager;
        this.cliPathsConfigLoader = cliPathsConfigLoader;
    }

    public void init(PrintWriter out, ColorScheme cs) {
        moduleManager.setOut(out);
        Optional<IgnitePaths> ignitePathsOpt = cliPathsConfigLoader.loadIgnitePathsConfig();
        if (ignitePathsOpt.isEmpty())
            initConfigFile();

        IgnitePaths cfg = cliPathsConfigLoader.loadIgnitePathsConfig().get();
        out.print("Creating directories... ");
        cfg.initOrRecover();
        out.println(Ansi.AUTO.string("@|green,bold Done!|@"));

        Table table = new Table(0, cs);

        table.addRow("@|bold Binaries Directory|@", cfg.binDir);
        table.addRow("@|bold Work Directory|@", cfg.workDir);

        out.println(table);
        out.println();

        installIgnite(cfg);
        initDefaultServerConfigs(cfg.serverDefaultConfigFile());
        out.println();
        out.println("Apache Ignite is successfully initialized. Use the " +
            cs.commandText("ignite node start") + " command to start a new local node.");
    }

    private void initDefaultServerConfigs(Path serverCfgFile) {
        try {
            if (!serverCfgFile.toFile().exists())
                Files.copy(
                    InitIgniteCommand.class
                        .getResourceAsStream("/default-config.xml"), serverCfgFile);
        }
        catch (IOException e) {
            throw new IgniteCLIException("Can't create default config file for server", e);
        }
    }

    private void installIgnite(IgnitePaths ignitePaths) {
        moduleManager.addModule("_server", ignitePaths, Collections.emptyList());
    }

    private File initConfigFile() {
        Path newCfgPath = pathResolver.osHomeDirectoryPath().resolve(".ignitecfg");
        File newCfgFile = newCfgPath.toFile();
        try {
            newCfgFile.createNewFile();
            Path binDir = pathResolver.osHomeDirectoryPath().resolve("ignite").resolve("bin");
            Path workDir = pathResolver.osHomeDirectoryPath().resolve("ignite").resolve("work");
            fillNewConfigFile(newCfgFile, binDir, workDir);
            return newCfgFile;
        }
        catch (IOException e) {
            throw new IgniteCLIException("Can't create configuration file in current directory: " + newCfgPath);
        }
    }

    private void fillNewConfigFile(File f, @NotNull Path binDir, @NotNull Path workDir) {
        try (FileWriter fileWriter = new FileWriter(f)) {
            Properties properties = new Properties();
            properties.setProperty("bin", binDir.toString());
            properties.setProperty("work", workDir.toString());
            properties.store(fileWriter, "");
        }
        catch (IOException e) {
            throw new IgniteCLIException("Can't write to ignitecfg file");
        }
    }
}
