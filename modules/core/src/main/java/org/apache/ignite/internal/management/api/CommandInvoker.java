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

package org.apache.ignite.internal.management.api;

import java.util.Collection;
import java.util.function.Consumer;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.jetbrains.annotations.Nullable;

import static java.util.Collections.singletonList;

/**
 * Command invoker.
 */
public class CommandInvoker<A extends IgniteDataTransferObject> {
    /** */
    private final @Nullable IgniteEx ignite;

    /** Command to execute. */
    protected final Command<A, ?> cmd;

    /** Parsed argument. */
    protected final A arg;

    /**
     * @param cmd Command to execute.
     * @param arg Argument
     * @param ignite Optional ignite instance.
     */
    public CommandInvoker(Command<A, ?> cmd, A arg, @Nullable IgniteEx ignite) {
        this.cmd = cmd;
        this.arg = arg;
        this.ignite = ignite;
    }

    /**
     * @param printer Result printer.
     * @return {@code True} of command successfully prepared and can be invoked, {@code false} otherwise.
     * @throws GridClientException In failed.
     */
    public boolean prepare(Consumer<String> printer) throws GridClientException {
        if (!(cmd instanceof PreparableCommand))
            return true;

        return ((PreparableCommand<A, ?>)cmd).prepare(client(), ignite, arg, printer);
    }

    /**
     * Actual command execution with verbose mode if required.
     *
     * @param printer Result printer.
     * @param verbose Use verbose mode or not
     * @return Result of operation (mostly usable for tests).
     * @throws GridClientException In failed.
     */
    public <R> R invoke(Consumer<String> printer, boolean verbose) throws GridClientException {
        R res;

        if (cmd instanceof LocalCommand)
            res = ((LocalCommand<A, R>)cmd).execute(client(), ignite, arg, printer);
        else if (cmd instanceof ComputeCommand) {
            ComputeCommand<A, R> cmd = (ComputeCommand<A, R>)this.cmd;

            Collection<GridClientNode> cmdNodes = cmd.nodes(CommandUtils.nodes(client(), ignite), arg);

            if (cmdNodes == null)
                cmdNodes = singletonList(defaultNode());

            res = CommandUtils.execute(client(), ignite, cmd.taskClass(), arg, cmdNodes);

            cmd.printResult(arg, res, printer);
        }
        else
            throw new IllegalArgumentException("Unknown command type: " + cmd);

        return res;
    }

    /** @return Default node to execute commands. */
    protected GridClientNode defaultNode() throws GridClientException {
        return CommandUtils.clusterToClientNode(ignite.localNode());
    }

    /**
     * @return Grid thin client instance which is already connected to cluster.
     * @throws GridClientException If failed.
     */
    protected @Nullable GridClient client() throws GridClientException {
        return null;
    }
}
