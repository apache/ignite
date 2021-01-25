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

package org.apache.ignite.cli.builtins.module;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.jar.JarInputStream;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigValue;
import org.apache.ignite.cli.CliVersionInfo;
import org.apache.ignite.cli.IgniteCLIException;
import org.apache.ignite.cli.IgnitePaths;
import picocli.CommandLine.Help.ColorScheme;

/**
 * Ignite distributive module manager.
 * The main responsibilities:
 * <ul>
 *     <li>Add/remove standard Ignite server modules</li>
 *     <li>Add/remove any external server modules from maven repositories.</li>
 *     <li>Add/remove Ignite CLI modules.</li>
 * </ul>
 */
@Singleton
public class ModuleManager {
    /** Prefix to identify internal modules in builtin modules list. */
    public static final String INTERNAL_MODULE_PREFIX = "_";

    /** Jar manifest key to identify the CLI module jar. */
    public static final String CLI_MODULE_MANIFEST_HEADER = "Apache-Ignite-CLI-Module";

    /** Maven artifact resolver. */
    private final MavenArtifactResolver mavenArtifactRslvr;

    /** Current Ignite CLI version. */
    private final CliVersionInfo cliVerInfo;

    /** Storage of meta info of installed modules. */
    private final ModuleRegistry moduleRegistry;

    /** List of standard Ignite modules. */
    private final List<StandardModuleDefinition> modules;

    /** Print writer for output user messages. */
    private PrintWriter out;

    /** Color scheme to enrich user output. */
    private ColorScheme cs;

    /**
     * Creates module manager instance.
     *
     * @param mavenArtifactRslvr Maven artifact resolver.
     * @param cliVerInfo Current Ignite CLI version info.
     * @param moduleRegistry Module storage.
     */
    @Inject
    public ModuleManager(
        MavenArtifactResolver mavenArtifactRslvr, CliVersionInfo cliVerInfo,
        ModuleRegistry moduleRegistry) {
        modules = readBuiltinModules();
        this.mavenArtifactRslvr = mavenArtifactRslvr;
        this.cliVerInfo = cliVerInfo;
        this.moduleRegistry = moduleRegistry;
    }

    /**
     * Sets print writer for any user messages.
     *
     * @param out PrintWriter
     */
    public void setOut(PrintWriter out) {
        this.out = out;

        mavenArtifactRslvr.setOut(out);
    }

    /**
     * Sets color scheme for enrhiching user output.
     *
     * @param cs ColorScheme
     */
    public void setColorScheme(ColorScheme cs) {
        this.cs = cs;
    }

    /**
     * Installs the CLI/server module by name to according directories from {@link IgnitePaths}.
     *
     * @param name Module name can be either fully qualified maven artifact name (groupId:artifactId:version)
     *             or the name of the standard Ignite module.
     * @param ignitePaths Ignite paths instance.
     * @param repositories Custom maven repositories to resolve module from.
     */
    public void addModule(String name, IgnitePaths ignitePaths, List<URL> repositories) {
        Path installPath = ignitePaths.libsDir();

        if (name.startsWith("mvn:")) {
            MavenCoordinates mavenCoordinates = MavenCoordinates.of(name);

            try {
                ResolveResult resolveRes = mavenArtifactRslvr.resolve(
                    installPath,
                    mavenCoordinates.grpId,
                    mavenCoordinates.artifactId,
                    mavenCoordinates.ver,
                    repositories
                );

                String mvnName = String.join(":", mavenCoordinates.grpId,
                    mavenCoordinates.artifactId, mavenCoordinates.ver);

                var isCliModule = isRootArtifactCliModule(
                    mavenCoordinates.artifactId, mavenCoordinates.ver,
                    resolveRes.artifacts());

                moduleRegistry.saveModule(new ModuleRegistry.ModuleDefinition(
                    mvnName,
                    (isCliModule) ? Collections.emptyList() : resolveRes.artifacts(),
                    (isCliModule) ? resolveRes.artifacts() : Collections.emptyList(),
                    ModuleRegistry.SourceType.Maven,
                    name
                ));

                out.println();
                out.println("New Maven dependency successfully added. To remove, type: " +
                    cs.commandText("ignite module remove ") + cs.parameterText(mvnName));
            }
            catch (IOException e) {
                throw new IgniteCLIException("\nFailed to install " + name, e);
            }
        }
        else if (isBuiltinModuleName(name)) {
            StandardModuleDefinition moduleDesc = readBuiltinModules()
                .stream()
                .filter(m -> m.name.equals(name))
                .findFirst().get();

            List<ResolveResult> libsResolveResults = new ArrayList<>();

            for (String artifact: moduleDesc.artifacts) {
                MavenCoordinates mavenCoordinates = MavenCoordinates.of(artifact, cliVerInfo.ver);

                try {
                    libsResolveResults.add(mavenArtifactRslvr.resolve(
                        ignitePaths.libsDir(),
                        mavenCoordinates.grpId,
                        mavenCoordinates.artifactId,
                        mavenCoordinates.ver,
                        repositories
                    ));
                }
                catch (IOException e) {
                    throw new IgniteCLIException("\nFailed to install an Ignite module: " + name, e);
                }
            }

            List<ResolveResult> cliResolvResults = new ArrayList<>();

            for (String artifact: moduleDesc.cliArtifacts) {
                MavenCoordinates mavenCoordinates = MavenCoordinates.of(artifact, cliVerInfo.ver);

                try {
                    cliResolvResults.add(mavenArtifactRslvr.resolve(
                        ignitePaths.cliLibsDir(),
                        mavenCoordinates.grpId,
                        mavenCoordinates.artifactId,
                        mavenCoordinates.ver,
                        repositories
                    ));
                }
                catch (IOException e) {
                    throw new IgniteCLIException("\nFailed to install a module " + name, e);
                }
            }

            try {
                moduleRegistry.saveModule(new ModuleRegistry.ModuleDefinition(
                    name,
                    libsResolveResults.stream().flatMap(r -> r.artifacts().stream()).collect(Collectors.toList()),
                    cliResolvResults.stream().flatMap(r -> r.artifacts().stream()).collect(Collectors.toList()),
                    ModuleRegistry.SourceType.Standard,
                    name
                ));
            }
            catch (IOException e) {
                throw new IgniteCLIException("Error during saving the installed module info");
            }

        }
        else {
            throw new IgniteCLIException(
                "Module coordinates for non-standard modules must be started with mvn:|file://");
        }
    }

    /**
     * Removes module by name.
     *
     * @param name Module name can be either the name of standard Ignite module
     *             or the fully qualified maven artifact 'groupId:artifactId:version'.
     * @return true if module was removed, false otherwise.
     */
    public boolean removeModule(String name) {
        try {
            return moduleRegistry.removeModule(name);
        }
        catch (IOException e) {
            throw new IgniteCLIException(
                "Can't remove module " + name, e);
        }
    }

    /**
     * Returns builtin Ignite modules list.
     * Builtin modules list packaged with CLI tool and so, depends only on its' version.
     *
     * @return Builtin modules list.
     */
    public List<StandardModuleDefinition> builtinModules() {
        return Collections.unmodifiableList(modules);
    }

    /**
     * Checks if root artifact contains Jar manifest key, which marks it as CLI extension.
     *
     * @param artifactId Maven artifact id.
     * @param ver Maven root artifact version.
     * @param artifacts Paths for all dependencies' files of the artifact.
     * @return true if the artifact has CLI mark, false otherwise.
     * @throws IOException If can't read artifact manifest.
     */
    private boolean isRootArtifactCliModule(String artifactId, String ver, List<Path> artifacts) throws IOException {
       var rootJarArtifactOpt = artifacts.stream()
           .filter(p -> MavenArtifactResolver.fileNameByArtifactPattern(artifactId, ver).equals(p.getFileName().toString()))
           .findFirst();

       if (rootJarArtifactOpt.isPresent()) {
           try (var input = new FileInputStream(rootJarArtifactOpt.get().toFile())) {
               var jarStream = new JarInputStream(input);
               var manifest = jarStream.getManifest();

               return "true".equals(manifest.getMainAttributes().getValue(CLI_MODULE_MANIFEST_HEADER));
           }
       }
       else
           return false;
    }

    /**
     * Checks if the module is a builtin Ignite module.
     *
     * @param name Module name.
     * @return true if the module is a builtin Ignite module.
     */
    private boolean isBuiltinModuleName(String name) {
        return readBuiltinModules().stream().anyMatch(m -> m.name.equals(name));
    }

    /**
     * Reads builtin modules from builtin modules registry.
     *
     * @return List of builtin modules.
     */
    private static List<StandardModuleDefinition> readBuiltinModules() {
        var cfg = ConfigFactory.load("builtin_modules.conf").getObject("modules");

        List<StandardModuleDefinition> modules = new ArrayList<>();

        for (Map.Entry<String, ConfigValue> entry: cfg.entrySet()) {
            ConfigObject cfgObj = (ConfigObject) entry.getValue();

            modules.add(new StandardModuleDefinition(
                entry.getKey(),
                cfgObj.toConfig().getString("description"),
                cfgObj.toConfig().getStringList("artifacts"),
                cfgObj.toConfig().getStringList("cli-artifacts")
            ));
        }

        return modules;
    }
}
