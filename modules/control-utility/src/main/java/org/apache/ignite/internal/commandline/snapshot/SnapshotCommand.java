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

package org.apache.ignite.internal.commandline.snapshot;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.commandline.AbstractCommand;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager;
import org.apache.ignite.mxbean.SnapshotMXBean;

import static org.apache.ignite.internal.commandline.CommandList.SNAPSHOT;

/**
 * control.sh Snapshot command.
 *
 * @see SnapshotMXBean
 * @see IgniteSnapshotManager
 */
public class SnapshotCommand extends AbstractCommand<Object> {
    /** Snapshot sub-command to execute. */
    private SnapshotSubcommand cmd;

    /** {@inheritDoc} */
    @Override public Object execute(ClientConfiguration clientCfg, IgniteLogger log) throws Exception {
        return cmd.execute(clientCfg, log);
    }

    /** {@inheritDoc} */
    @Override public Object arg() {
        return cmd.arg();
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        cmd = SnapshotSubcommands.of(argIter.nextArg("Expected snapshot action.")).subCommand();

        cmd.parseArguments(argIter);
    }

    /** {@inheritDoc} */
    @Override public void printUsage(IgniteLogger log) {
        for (SnapshotSubcommands cmd : SnapshotSubcommands.values())
            cmd.subCommand().printUsage(log);
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt() {
        return cmd == null ? null : cmd.confirmationPrompt();
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return SNAPSHOT.toCommandName();
    }
}
