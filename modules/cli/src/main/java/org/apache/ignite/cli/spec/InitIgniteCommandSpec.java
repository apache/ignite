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

/**
 * Command for init Ignite distributive to start new nodes on the current machine.
 *
 * @see IgniteCommand
 */
@CommandLine.Command(name = "init", description = "Installs Ignite core modules locally.")
public class InitIgniteCommandSpec extends CommandSpec implements IgniteCommand {
    /** Init command implementation. */
    @Inject
    private InitIgniteCommand cmd;

    /** Option for custom maven repository to download Ignite core. */
    @CommandLine.Option(
        names = "--repo",
        description = "Additional Maven repository URL"
    )
    private URL[] urls;

    /** {@inheritDoc} */
    @Override public void run() {
        cmd.init(urls, spec.commandLine().getOut(), spec.commandLine().getColorScheme());
    }
}
