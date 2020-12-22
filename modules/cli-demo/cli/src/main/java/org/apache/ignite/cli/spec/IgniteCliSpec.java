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

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import io.micronaut.context.ApplicationContext;
import org.apache.ignite.cli.CliPathsConfigLoader;
import org.apache.ignite.cli.CommandFactory;
import org.apache.ignite.cli.ErrorHandler;
import org.apache.ignite.cli.HelpFactoryImpl;
import org.apache.ignite.cli.IgniteCLIException;
import org.apache.ignite.cli.InteractiveWrapper;
import org.apache.ignite.cli.builtins.module.ModuleStorage;
import org.apache.ignite.cli.common.IgniteCommand;
import picocli.CommandLine;

/**
 *
 */
@CommandLine.Command(
    name = "ignite",
    description = "Type @|green ignite <COMMAND>|@ @|yellow --help|@ to get help for any command.",
    subcommands = {
        InitIgniteCommandSpec.class,
        ModuleCommandSpec.class,
        NodeCommandSpec.class,
        ConfigCommandSpec.class,
    }
)
public class IgniteCliSpec extends CommandSpec {
    @CommandLine.Option(names = "-i", hidden = true, required = false)
    boolean interactive;

    @Override public void run() {
        CommandLine cli = spec.commandLine();

        cli.getOut().print(banner());

        if (interactive)
            new InteractiveWrapper().run(cli);
        else
            cli.usage(cli.getOut());
    }

    public static CommandLine initCli(ApplicationContext applicationContext) {
        CommandLine.IFactory factory = new CommandFactory(applicationContext);
        ErrorHandler errorHandler = applicationContext.createBean(ErrorHandler.class);
        CommandLine cli = new CommandLine(IgniteCliSpec.class, factory)
            .setExecutionExceptionHandler(errorHandler)
            .setParameterExceptionHandler(errorHandler);

        cli.setHelpFactory(new HelpFactoryImpl());

        cli.setColorScheme(new CommandLine.Help.ColorScheme.Builder()
            .commands(CommandLine.Help.Ansi.Style.fg_green)
            .options(CommandLine.Help.Ansi.Style.fg_yellow)
            .parameters(CommandLine.Help.Ansi.Style.fg_cyan)
            .errors(CommandLine.Help.Ansi.Style.fg_red, CommandLine.Help.Ansi.Style.bold)
            .build());

        applicationContext.createBean(CliPathsConfigLoader.class)
            .loadIgnitePathsConfig()
            .ifPresent(ignitePaths ->
                {
                    try {
                        loadSubcommands(
                            cli,
                            applicationContext.createBean(ModuleStorage.class)
                                .listInstalled()
                                .modules
                                .stream()
                                .flatMap(m -> m.cliArtifacts.stream())
                                .collect(Collectors.toList()));
                    }
                    catch (IOException e) {
                        throw new IgniteCLIException("Can't load cli modules due to IO error");
                    }
                }
            );
        return cli;
    }

    public static void loadSubcommands(CommandLine commandLine, List<Path> cliLibs) {
        URL[] urls = cliLibs.stream()
            .map(p -> {
                try {
                    return p.toUri().toURL();
                }
                catch (MalformedURLException e) {
                    throw new IgniteCLIException("Can't convert cli module path to URL for loading by classloader");
                }
            }).toArray(URL[]::new);
        ClassLoader classLoader = new URLClassLoader(urls,
            IgniteCliSpec.class.getClassLoader());
        ServiceLoader<IgniteCommand> loader = ServiceLoader.load(IgniteCommand.class, classLoader);
        loader.reload();
        for (IgniteCommand igniteCommand: loader) {
            commandLine.addSubcommand(igniteCommand);
        }
    }
}
