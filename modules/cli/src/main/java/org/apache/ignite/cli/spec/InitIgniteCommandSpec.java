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
import javax.inject.Inject;
import org.apache.ignite.cli.common.IgniteCommand;
import org.apache.ignite.cli.builtins.init.InitIgniteCommand;
import picocli.CommandLine;

@CommandLine.Command(name = "init", description = "Installs Ignite core modules locally.")
public class InitIgniteCommandSpec extends CommandSpec implements IgniteCommand {

    @Inject
    InitIgniteCommand command;

    @CommandLine.Option(
        names = "--repo",
        description = "Additional Maven repository URL"
    )
    public URL[] urls;

    @Override public void run() {
        command.init(urls, spec.commandLine().getOut(), spec.commandLine().getColorScheme());
    }
}
