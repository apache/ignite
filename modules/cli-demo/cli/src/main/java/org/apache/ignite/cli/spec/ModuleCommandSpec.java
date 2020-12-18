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

import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;
import javax.inject.Inject;
import com.github.freva.asciitable.AsciiTable;
import com.github.freva.asciitable.Column;
import com.github.freva.asciitable.HorizontalAlign;
import io.micronaut.context.ApplicationContext;
import org.apache.ignite.cli.CliPathsConfigLoader;
import org.apache.ignite.cli.builtins.module.ModuleManager;
import org.apache.ignite.cli.common.IgniteCommand;
import picocli.CommandLine;

@CommandLine.Command(
    name = "module",
    description = "Manage optional Ignite modules and external artifacts.",
    subcommands = {
        ModuleCommandSpec.ListModuleCommandSpec.class,
        ModuleCommandSpec.AddModuleCommandSpec.class,
        ModuleCommandSpec.RemoveModuleCommandSpec.class
    }
)
public class ModuleCommandSpec extends AbstractCommandSpec implements IgniteCommand {

    @Override public void run() {
        spec.commandLine().usage(spec.commandLine().getOut());
    }

    @CommandLine.Command(name = "add", description = "Add an optional Ignite module or an external artifact.")
    public static class AddModuleCommandSpec extends AbstractCommandSpec {

        @Inject private ModuleManager moduleManager;

        @Inject
        private CliPathsConfigLoader cliPathsConfigLoader;

        @CommandLine.Option(names = "--repo",
            description = "Url to custom maven repo")
        public URL[] urls;

        @CommandLine.Parameters(paramLabel = "module",
            description = "can be a 'builtin module name (see module list)'|'mvn:groupId:artifactId:version'")
        public String moduleName;

        @Override public void run() {
            var ignitePaths = cliPathsConfigLoader.loadIgnitePathsOrThrowError();
            moduleManager.setOut(spec.commandLine().getOut());
            moduleManager.addModule(moduleName,
                ignitePaths,
                (urls == null)? Collections.emptyList() : Arrays.asList(urls));
        }
    }

    @CommandLine.Command(name = "remove", description = "Add an optional Ignite module or an external artifact.")
    public static class RemoveModuleCommandSpec extends AbstractCommandSpec {

        @Inject private ModuleManager moduleManager;

        @CommandLine.Parameters(paramLabel = "module",
            description = "can be a 'builtin module name (see module list)'|'mvn:groupId:artifactId:version'")
        public String moduleName;

        @Override public void run() {
            if (moduleManager.removeModule(moduleName))
                spec.commandLine().getOut().println("Module " + moduleName + " was removed successfully");
            else
                spec.commandLine().getOut().println("Module " + moduleName + " is not found");
        }
    }

    @CommandLine.Command(name = "list", description = "Show the list of available optional Ignite modules.")
    public static class ListModuleCommandSpec extends AbstractCommandSpec {

        @Inject private ModuleManager moduleManager;

        @Override public void run() {
            var builtinModules = moduleManager.builtinModules()
                .stream()
                .filter(m -> !m.name.startsWith(ModuleManager.INTERNAL_MODULE_PREFIX));
            String table = AsciiTable.getTable(builtinModules.collect(Collectors.toList()), Arrays.asList(
                new Column().header("Name").dataAlign(HorizontalAlign.LEFT).with(m -> m.name),
                new Column().header("Description").dataAlign(HorizontalAlign.LEFT).with(m -> m.description)
            ));
            spec.commandLine().getOut().println(table);
        }
    }

}
