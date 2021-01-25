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
import java.nio.file.Path;

/**
 * The main resolver of Ignite paths for the current installation (like bin, work and etc. dirs).
 * Current Ignite distributive has the following dirs structure:
 * <ul>
 *     <li>bin</li>
 *     <ul>
 *         <li>${version}</li>
 *         <ul>
 *             <li>cli</li>
 *             <li>libs</li>
 *         </ul>
 *     </ul>
 *     <li>work</li>
 *     <ul>
 *         <li>config</li>
 *         <ul>
 *             <li>default-config.xml</li>
 *         </ul>
 *         <li>cli</li>
 *         <ul>
 *             <li>pids</li>
 *         </ul>
 *         <li>modules.json</li>
 *     </ul>
 * </ul>
 */
public class IgnitePaths {
    /**
     * Path to Ignite bin directory.
     * Bin directory contains jar artifacts for Ignite server and CLI modules.
     */
    public final Path binDir;

    /** Work directory for Ignite server and CLI operation. */
    public final Path workDir;

    /**
     * Ignite CLI version.
     * Also, the same version will be used for addressing any binaries inside bin dir
     */
    private final String ver;

    /**
     * Creates resolved ignite paths instance from Ignite CLI version and base dirs paths.
     *
     * @param binDir Bin directory.
     * @param workDir Work directory.
     * @param ver Ignite CLI version.
     */
    public IgnitePaths(Path binDir, Path workDir, String ver) {
        this.binDir = binDir;
        this.workDir = workDir;
        this.ver = ver;
    }

    /**
     * Path where CLI module artifacts will be placed.
     */
    public Path cliLibsDir() {
        return binDir.resolve(ver).resolve("cli");
    }

    /**
     * Path where Ignite server module artifacts will be placed.
     */
    public Path libsDir() {
        return binDir.resolve(ver).resolve("libs");
    }

    /**
     * Path where Ignite node pid files will be created.
     */
    public Path cliPidsDir() {
        return workDir.resolve("cli").resolve("pids");
    }

    /**
     * Path to file with registry data for {@link org.apache.ignite.cli.builtins.module.ModuleRegistry}
     */
    public Path installedModulesFile() {
        return workDir.resolve("modules.json");
    }

    /**
     * Path to directory with Ignite nodes configs.
     */
    public Path serverConfigDir() {
        return workDir.resolve("config");
    }

    /**
     * Path to default Ignite node config.
     */
    public Path serverDefaultConfigFile() {
        return serverConfigDir().resolve("default-config.xml");
    }

    /**
     * Init or recovers Ignite distributive directories structure.
     */
    public void initOrRecover() {
        File igniteWork = workDir.toFile();
        if (!(igniteWork.exists() || igniteWork.mkdirs()))
            throw new IgniteCLIException("Can't create working directory: " + workDir);

        File igniteBin = libsDir().toFile();
        if (!(igniteBin.exists() || igniteBin.mkdirs()))
            throw new IgniteCLIException("Can't create a directory for ignite modules: " + libsDir());

        File igniteBinCli = cliLibsDir().toFile();
        if (!(igniteBinCli.exists() || igniteBinCli.mkdirs()))
            throw new IgniteCLIException("Can't create a directory for cli modules: " + cliLibsDir());

        File srvCfg = serverConfigDir().toFile();
        if (!(srvCfg.exists() || srvCfg.mkdirs()))
            throw new IgniteCLIException("Can't create a directory for server configs: " + serverConfigDir());
    }

    /**
     * Validates that all Ignite distributive directories is exists.
     *
     * @return true if check passes, false otherwise.
     */
    public boolean validateDirs() {
        return workDir.toFile().exists() &&
                libsDir().toFile().exists() &&
                cliLibsDir().toFile().exists() &&
                serverConfigDir().toFile().exists();
    }
}
