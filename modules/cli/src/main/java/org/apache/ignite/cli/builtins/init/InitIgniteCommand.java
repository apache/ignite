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
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import javax.inject.Inject;
import org.apache.ignite.cli.CliPathsConfigLoader;
import org.apache.ignite.cli.IgniteCLIException;
import org.apache.ignite.cli.IgnitePaths;
import org.apache.ignite.cli.Table;
import org.apache.ignite.cli.builtins.SystemPathResolver;
import org.apache.ignite.cli.builtins.module.ModuleManager;
import org.jetbrains.annotations.NotNull;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Help.ColorScheme;

/**
 * Implementation of command for initializing Ignite distro on the current machine.
 * This process has the following steps:
 * <ul>
 *     <li>Initialize configuration file with the needed directories paths (@see {@link IgnitePaths})</li>
 *     <li>Create all needed directories for Ignite deployment</li>
 *     <li>Download current Ignite distro and prepare it for running</li>
 * </ul>
 */
public class InitIgniteCommand {
    /** Resolver of paths like home directory and etc. **/
    private final SystemPathResolver pathRslvr;

    /** Manager of Ignite server and CLI modules. **/
    private final ModuleManager moduleMgr;

    /** Loader of current Ignite distro dirs configuration. **/
    private final CliPathsConfigLoader cliPathsCfgLdr;

    /**
     * Creates init command instance.
     *
     * @param pathRslvr Resolver of paths like home directory and etc.
     * @param moduleMgr Manager of Ignite server and CLI modules.
     * @param cliPathsCfgLdr Loader of current Ignite distro dirs configuration.
     */
    @Inject
    public InitIgniteCommand(
        SystemPathResolver pathRslvr,
        ModuleManager moduleMgr,
        CliPathsConfigLoader cliPathsCfgLdr) {
        this.pathRslvr = pathRslvr;
        this.moduleMgr = moduleMgr;
        this.cliPathsCfgLdr = cliPathsCfgLdr;
    }

    /**
     * Executes init process with initialization of config file,
     * directories, and download of current Ignite release.
     * Also, it can be used to recover after corruption of node directories structure.
     *
     * @param urls Urls with custom maven repositories for Ignite download.
     * @param out PrintWriter for output user message.
     * @param cs ColorScheme for enriching user outputs with colors.
     */
    public void init(URL[] urls, PrintWriter out, ColorScheme cs) {
        moduleMgr.setOut(out);

        Optional<IgnitePaths> ignitePathsOpt = cliPathsCfgLdr.loadIgnitePathsConfig();

        if (ignitePathsOpt.isEmpty())
            initConfigFile();

        IgnitePaths cfg = cliPathsCfgLdr.loadIgnitePathsConfig().get();

        out.print("Creating directories... ");

        cfg.initOrRecover();

        out.println(Ansi.AUTO.string("@|green,bold Done!|@"));

        Table tbl = new Table(0, cs);

        tbl.addRow("@|bold Binaries Directory|@", cfg.binDir);
        tbl.addRow("@|bold Work Directory|@", cfg.workDir);

        out.println(tbl);
        out.println();

        installIgnite(cfg, urls);

        initDefaultServerConfigs(cfg.serverDefaultConfigFile());

        out.println();
        out.println("Apache Ignite is successfully initialized. Use the " +
            cs.commandText("ignite node start") + " command to start a new local node.");
    }

    /**
     * Init default server config file.
     *
     * @param srvCfgFile Path to server node config file.
     */
    private void initDefaultServerConfigs(Path srvCfgFile) {
        try {
            if (!srvCfgFile.toFile().exists())
                Files.copy(
                    InitIgniteCommand.class
                        .getResourceAsStream("/default-config.xml"), srvCfgFile);
        }
        catch (IOException e) {
            throw new IgniteCLIException("Can't create default config file for server", e);
        }
    }

    /**
     * Downloads ignite node distro with all needed dependencies.
     *
     * @param ignitePaths Ignite distributive paths (bin, config, etc.).
     * @param urls Urls for custom maven repositories.
     */
    private void installIgnite(IgnitePaths ignitePaths, URL[] urls) {
        moduleMgr.addModule("_server", ignitePaths,
            urls == null ? Collections.emptyList() : Arrays.asList(urls));
    }

    /**
     * Init configuration file for CLI utility with Ignite directories (bin, config, etc.) paths.
     *
     * @return Initialized configuration file.
     */
    private File initConfigFile() {
        Path newCfgPath = pathRslvr.osHomeDirectoryPath().resolve(".ignitecfg");
        File newCfgFile = newCfgPath.toFile();

        try {
            newCfgFile.createNewFile();

            Path binDir = pathRslvr.toolHomeDirectoryPath().resolve("ignite-bin");
            Path workDir = pathRslvr.toolHomeDirectoryPath().resolve("ignite-work");

            fillNewConfigFile(newCfgFile, binDir, workDir);

            return newCfgFile;
        }
        catch (IOException e) {
            throw new IgniteCLIException("Can't create configuration file in current directory: " + newCfgPath);
        }
    }

    /**
     * Fills config file with bin and work directories paths.
     *
     * @param f Config file.
     * @param binDir Path for bin dir.
     * @param workDir Path for work dir.
     */
    private void fillNewConfigFile(File f, @NotNull Path binDir, @NotNull Path workDir) {
        try (FileWriter fileWriter = new FileWriter(f)) {
            Properties props = new Properties();

            props.setProperty("bin", binDir.toString());
            props.setProperty("work", workDir.toString());
            props.store(fileWriter, "");
        }
        catch (IOException e) {
            throw new IgniteCLIException("Can't write to ignitecfg file");
        }
    }
}
