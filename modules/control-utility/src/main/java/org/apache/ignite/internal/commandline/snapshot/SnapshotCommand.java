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

import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClientConfiguration;
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
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        return cmd.subcommand().execute(clientCfg, log);
    }

    /** {@inheritDoc} */
    @Override public Object arg() {
        return cmd.subcommand().arg();
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt() {
        return cmd.subcommand().confirmationPrompt();
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        cmd = SnapshotSubcommand.of(argIter.nextArg("Expected snapshot action."));

        assert cmd != null;

        cmd.subcommand().parseArguments(argIter);
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        for (SnapshotSubcommand subCmd : SnapshotSubcommand.values())
            subCmd.subcommand().printUsage(log);
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return SNAPSHOT.toCommandName();
    }
}
