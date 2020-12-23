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

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

@Singleton
public class ModuleManager {

    private final MavenArtifactResolver mavenArtifactResolver;
    private final CliVersionInfo cliVersionInfo;
    private final ModuleStorage moduleStorage;
    private final List<StandardModuleDefinition> modules;

    public static final String INTERNAL_MODULE_PREFIX = "_";

    private PrintWriter out;
    private ColorScheme cs;

    @Inject
    public ModuleManager(
        MavenArtifactResolver mavenArtifactResolver, CliVersionInfo cliVersionInfo,
        ModuleStorage moduleStorage) {
        modules = readBuiltinModules();
        this.mavenArtifactResolver = mavenArtifactResolver;
        this.cliVersionInfo = cliVersionInfo;
        this.moduleStorage = moduleStorage;
    }

    public void setOut(PrintWriter out) {
        this.out = out;

        mavenArtifactResolver.setOut(out);
    }

    public void setColorScheme(ColorScheme cs) {
        this.cs = cs;
    }

    public void addModule(String name, IgnitePaths ignitePaths, List<URL> repositories) {
        Path installPath = ignitePaths.libsDir();
        if (name.startsWith("mvn:")) {
            MavenCoordinates mavenCoordinates = MavenCoordinates.of(name);

            try {
                ResolveResult resolveResult = mavenArtifactResolver.resolve(
                    installPath,
                    mavenCoordinates.groupId,
                    mavenCoordinates.artifactId,
                    mavenCoordinates.version,
                    repositories
                );

                String mvnName = String.join(":", mavenCoordinates.groupId,
                    mavenCoordinates.artifactId, mavenCoordinates.version);

                moduleStorage.saveModule(new ModuleStorage.ModuleDefinition(
                    mvnName,
                    resolveResult.artifacts(),
                    new ArrayList<>(),
                    ModuleStorage.SourceType.Maven,
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
        else if (isStandardModuleName(name)) {
            StandardModuleDefinition moduleDescription = readBuiltinModules()
                .stream()
                .filter(m -> m.name.equals(name))
                .findFirst().get();
            List<ResolveResult> libsResolveResults = new ArrayList<>();
            for (String artifact: moduleDescription.artifacts) {
                MavenCoordinates mavenCoordinates = MavenCoordinates.of(artifact, cliVersionInfo.version);
                try {
                    libsResolveResults.add(mavenArtifactResolver.resolve(
                        ignitePaths.libsDir(),
                        mavenCoordinates.groupId,
                        mavenCoordinates.artifactId,
                        mavenCoordinates.version,
                        repositories
                    ));
                }
                catch (IOException e) {
                    throw new IgniteCLIException("\nFailed to install an Ignite module: " + name, e);
                }
            }

            List<ResolveResult> cliResolvResults = new ArrayList<>();
            for (String artifact: moduleDescription.cliArtifacts) {
                MavenCoordinates mavenCoordinates = MavenCoordinates.of(artifact, cliVersionInfo.version);
                try {
                    cliResolvResults.add(mavenArtifactResolver.resolve(
                        ignitePaths.cliLibsDir(),
                        mavenCoordinates.groupId,
                        mavenCoordinates.artifactId,
                        mavenCoordinates.version,
                        repositories
                    ));
                }
                catch (IOException e) {
                    throw new IgniteCLIException("\nFailed to install a module " + name, e);
                }
            }

            try {
                moduleStorage.saveModule(new ModuleStorage.ModuleDefinition(
                    name,
                    libsResolveResults.stream().flatMap(r -> r.artifacts().stream()).collect(Collectors.toList()),
                    cliResolvResults.stream().flatMap(r -> r.artifacts().stream()).collect(Collectors.toList()),
                    ModuleStorage.SourceType.Standard,
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

    public boolean removeModule(String name) {
        try {
            return moduleStorage.removeModule(name);
        }
        catch (IOException e) {
            throw new IgniteCLIException(
                "Can't remove module " + name, e);
        }
    }

    public List<StandardModuleDefinition> builtinModules() {
        return modules;
    }

    private boolean isStandardModuleName(String name) {
        return readBuiltinModules().stream().anyMatch(m -> m.name.equals(name));
    }



    private static List<StandardModuleDefinition> readBuiltinModules() {
        com.typesafe.config.ConfigObject config = ConfigFactory.load("builtin_modules.conf").getObject("modules");
        List<StandardModuleDefinition> modules = new ArrayList<>();
        for (Map.Entry<String, ConfigValue> entry: config.entrySet()) {
            ConfigObject configObject = (ConfigObject) entry.getValue();
            modules.add(new StandardModuleDefinition(
                entry.getKey(),
                configObject.toConfig().getString("description"),
                configObject.toConfig().getStringList("artifacts"),
                configObject.toConfig().getStringList("cli-artifacts")
            ));
        }
        return modules;
    }

}
