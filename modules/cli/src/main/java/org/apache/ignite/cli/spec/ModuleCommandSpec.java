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

import java.io.PrintWriter;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.stream.Collectors;
import javax.inject.Inject;
import org.apache.ignite.cli.CliPathsConfigLoader;
import org.apache.ignite.cli.Table;
import org.apache.ignite.cli.builtins.module.MavenCoordinates;
import org.apache.ignite.cli.builtins.module.ModuleManager;
import org.apache.ignite.cli.builtins.module.ModuleRegistry;
import org.apache.ignite.cli.builtins.module.StandardModuleDefinition;
import org.apache.ignite.cli.common.IgniteCommand;
import picocli.CommandLine;
import picocli.CommandLine.Help.ColorScheme;

/**
 * Commands for managing Ignite modules.
 */
@CommandLine.Command(
    name = "module",
    description = "Manages optional Ignite modules and additional Maven dependencies.",
    subcommands = {
        ModuleCommandSpec.ListModuleCommandSpec.class,
        ModuleCommandSpec.AddModuleCommandSpec.class,
        ModuleCommandSpec.RemoveModuleCommandSpec.class
    }
)
public class ModuleCommandSpec extends CategorySpec implements IgniteCommand {
    /** Command for add Ignite modules. */
    @CommandLine.Command(
        name = "add",
        description = "Adds an optional Ignite module or an additional Maven dependency."
    )
    public static class AddModuleCommandSpec extends CommandSpec {

        /** Module manager. */
        @Inject
        private ModuleManager moduleMgr;

        /** Loader of ignite distribution paths' confg. */
        @Inject
        private CliPathsConfigLoader cliPathsCfgLdr;

        /** Command option for setting custom maven repository for module lookup. */
        @CommandLine.Option(
            names = "--repo",
            description = "Additional Maven repository URL"
        )
        private URL[] urls;

        /** Module name command parameter. */
        @CommandLine.Parameters(
            paramLabel = "module",
            description = "Optional Ignite module name or Maven dependency coordinates (mvn:groupId:artifactId:version)"
        )
        private String moduleName;

        /** {@inheritDoc} */
        @Override public void run() {
            var ignitePaths = cliPathsCfgLdr.loadIgnitePathsOrThrowError();

            moduleMgr.setOut(spec.commandLine().getOut());
            moduleMgr.setColorScheme(spec.commandLine().getColorScheme());

            moduleMgr.addModule(moduleName,
                ignitePaths,
                (urls == null) ? Collections.emptyList() : Arrays.asList(urls));
        }
    }

    /**
     * Command for removing installed Ignite modules.
     */
    @CommandLine.Command(
        name = "remove",
        description = "Removes an optional Ignite module or an additional Maven dependency."
    )
    public static class RemoveModuleCommandSpec extends CommandSpec {

        /** Module manager. */
        @Inject
        private ModuleManager moduleMgr;

        /** Module name command parameter. */
        @CommandLine.Parameters(
            paramLabel = "module",
            description = "Optional Ignite module name or Maven dependency coordinates (groupId:artifactId:version)"
        )
        private String moduleName;

        /** {@inheritDoc} */
        @Override public void run() {
            PrintWriter out = spec.commandLine().getOut();
            ColorScheme cs = spec.commandLine().getColorScheme();

            if (moduleMgr.removeModule(moduleName))
                out.println("Module " + cs.parameterText(moduleName) + " was removed successfully.");
            else
                out.println("Nothing to do: module " + cs.parameterText(moduleName) + " is not yet added.");
        }
    }

    /**
     * Shows available and installed Ignite modules.
     */
    @CommandLine.Command(
        name = "list",
        description = "Shows the list of Ignite modules and Maven dependencies."
    )
    public static class ListModuleCommandSpec extends CommandSpec {
        /** Module manager. */
        @Inject
        private ModuleManager moduleMgr;

        /** Module registry. */
        @Inject
        private ModuleRegistry moduleRegistry;

        /** {@inheritDoc} */
        @Override public void run() {
            var installedModules = new LinkedHashMap<String, ModuleRegistry.ModuleDefinition>();

            for (var m: moduleRegistry
                .listInstalled()
                .modules
            ) {
                installedModules.put(m.name, m);
            }

            PrintWriter out = spec.commandLine().getOut();
            ColorScheme cs = spec.commandLine().getColorScheme();

            var builtinModules = moduleMgr.builtinModules()
                .stream()
                .filter(m -> !m.name.startsWith(ModuleManager.INTERNAL_MODULE_PREFIX))
                .map(m -> new StandardModuleView(m, installedModules.containsKey(m.name)))
                .collect(Collectors.toList());

            out.println(cs.text("@|bold Optional Ignite Modules|@"));

            if (builtinModules.isEmpty())
                out.println("    Currently, there are no optional Ignite modules available for installation.");
            else {
                Table table = new Table(0, cs);

                table.addRow("@|bold Name|@", "@|bold Description|@", "@|bold Installed?|@");

                for (StandardModuleView m : builtinModules) {
                    table.addRow(m.standardModuleDefinition.name, m.standardModuleDefinition.desc,
                        m.installed ? "Yes" : "No");
                }

                out.println(table);
            }

            out.println();
            out.println(cs.text("@|bold Additional Maven Dependencies|@"));

            var externalInstalledModules = installedModules.values().stream()
                .filter(m -> !(m.type == ModuleRegistry.SourceType.Standard))
                .collect(Collectors.toList());

            if (externalInstalledModules.isEmpty()) {
                out.println("    No additional Maven dependencies installed. Use the " +
                    cs.commandText("ignite module add") + " command to add a dependency.");
            }
            else {
                Table table = new Table(0, cs);

                table.addRow("@|bold Group ID|@", "@|bold Artifact ID|@", "@|bold Version|@");

                for (ModuleRegistry.ModuleDefinition m :externalInstalledModules) {
                    MavenCoordinates mvn = MavenCoordinates.of("mvn:" + m.name);

                    table.addRow(mvn.grpId, mvn.artifactId, mvn.ver);
                }

                out.println(table);
                out.println("Type " + cs.commandText("ignite module remove") + " " +
                    cs.parameterText("<groupId>:<artifactId>:<version>") + " to remove a dependency.");
            }
        }

        /**
         * Simple tuple-like wrapper for (StandardModuleDefinition, installed) pairs.
         */
        private static class StandardModuleView {
            /** Module definition. */
            private final StandardModuleDefinition standardModuleDefinition;

            /** Installed flag. */
            private final boolean installed;

            /**
             * Creates (moduleDefinition, installed) pair.
             *
             * @param standardModuleDefinition Module definition.
             * @param installed true if module already installed, false otherwise.
             */
            public StandardModuleView(StandardModuleDefinition standardModuleDefinition, boolean installed) {
                this.standardModuleDefinition = standardModuleDefinition;
                this.installed = installed;
            }
        }
    }

}
