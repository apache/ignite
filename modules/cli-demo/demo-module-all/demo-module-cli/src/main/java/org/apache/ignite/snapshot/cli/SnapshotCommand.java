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

package org.apache.ignite.snapshot.cli;

import org.apache.ignite.cli.common.IgniteCommand;
import picocli.CommandLine;

@CommandLine.Command(
    name = "snapshot",
    description = "Snapshots management",
    subcommands = {
        SnapshotCommand.CreateSnashotCommand.class,
        SnapshotCommand.CancelSnapshotCommand.class}
)
public class SnapshotCommand implements IgniteCommand, Runnable {

    @CommandLine.Spec CommandLine.Model.CommandSpec spec;

    @Override public void run() {
        spec.commandLine().usage(spec.commandLine().getOut());
    }

    @CommandLine.Command(
        name = "create",
        description = "Create snapshot"
    )
    public static class CreateSnashotCommand implements Runnable {

        @CommandLine.Spec CommandLine.Model.CommandSpec spec;

        @Override public void run() {
            spec.commandLine().getOut().println("Create snapshot command was executed");
        }
    }


    @CommandLine.Command(
        name = "cancel",
        description = "Cancel snapshot"
    )
    public static class CancelSnapshotCommand implements Runnable {

        @CommandLine.Spec CommandLine.Model.CommandSpec spec;

        @Override public void run() {
            spec.commandLine().getOut().println("Cancel snapshot command was executed");
        }

    }
}
