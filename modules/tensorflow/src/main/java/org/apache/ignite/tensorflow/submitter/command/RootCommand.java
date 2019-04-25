/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.tensorflow.submitter.command;

import picocli.CommandLine;

/**
 * Root command that aggregates all sub commands.
 */
@CommandLine.Command(
    name = "ignite-tf",
    description = "Apache Ignite and TensorFlow integration command line tool that allows to start, maintain and" +
        " stop distributed deep learning utilizing Apache Ignite infrastructure and data.",
    subcommands = {
        StartCommand.class,
        StopCommand.class,
        AttachCommand.class,
        PsCommand.class
    },
    mixinStandardHelpOptions = true
)
public class RootCommand extends AbstractCommand {
    /** {@inheritDoc} */
    @Override public void run() {
        CommandLine.usage(this, System.out);
    }
}
