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
 *      <li>bin
 *          <ul>
 *              <li>${version}
 *                  <ul>
 *                      <li>cli</li>
 *                      <li>libs</li>
 *                  </ul>
 *              </li>
 *          </ul>
 *     </li>
 *     <li>work
 *          <ul>
 *              <li>config
 *                  <ul>
 *                      <li>default-config.xml</li>
 *                  </ul>
 *              </li>
 *              <li>cli
 *                  <ul>
 *                      <li>pids</li>
 *                  </ul>
 *              </li>
 *              <li>modules.json</li>
 *          </ul>
 *     </li>
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

    /** Directory for storing server node configs. */
    public final Path configDir;

    /** Directory for server nodes logs */
    public final Path logDir;

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
    public IgnitePaths(Path binDir, Path workDir, Path configDir, Path logDir, String ver) {
        this.binDir = binDir;
        this.workDir = workDir;
        this.configDir = configDir;
        this.logDir = logDir;
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
        return configDir.resolve("default-config.xml");
    }

    /**
     * Init or recovers Ignite distributive directories structure.
     */
    public void initOrRecover() {
        initDirIfNeeded(workDir,"Can't create working directory: " + workDir);
        initDirIfNeeded(binDir,"Can't create bin directory: " + binDir);
        initDirIfNeeded(libsDir(),"Can't create a directory for ignite modules: " + libsDir());
        initDirIfNeeded(cliLibsDir(),"Can't create a directory for cli modules: " + cliLibsDir());
        initDirIfNeeded(configDir,"Can't create a directory for server configs: " + configDir);
        initDirIfNeeded(logDir,"Can't create a directory for server logs: " + logDir);
    }

    /**
     * Create directory if not exists.
     *
     * @param dir Directory
     * @param exceptionMessage Exception message if directory wasn't created
     */
    private void initDirIfNeeded(Path dir, String exceptionMessage) {
        File dirFile = dir.toFile();
        if (!(dirFile.exists() || dirFile.mkdirs()))
            throw new IgniteCLIException(exceptionMessage);
    }

    /**
     * Validates that all Ignite distributive directories is exists.
     *
     * @return true if check passes, false otherwise.
     */
    public boolean validateDirs() {
        return workDir.toFile().exists() &&
                binDir.toFile().exists() &&
                libsDir().toFile().exists() &&
                cliLibsDir().toFile().exists() &&
                configDir.toFile().exists() &&
                logDir.toFile().exists();
    }
}
