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

package org.apache.ignite.internal.commandline;

import java.util.Optional;
import java.util.UUID;
import java.util.logging.Logger;

import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.commandline.persistence.cleaning.PersistenceCleaningArguments;
import org.apache.ignite.internal.commandline.persistence.cleaning.PersistenceCleaningSubcommands;
import org.apache.ignite.internal.visor.persistence.cleaning.PersistenceCleaningTask;
import org.apache.ignite.internal.visor.persistence.cleaning.PersistenceCleaningTaskArg;

import static org.apache.ignite.internal.commandline.CommandList.PERSISTENCE_CLEANING;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;

/** */
public class PersistenceCleaningCommand implements Command<PersistenceCleaningArguments> {
    /** */
    private PersistenceCleaningArguments cleaningArgs;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger logger) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            Optional<GridClientNode> firstNodeOpt = client.compute().nodes().stream().findFirst();

            if (firstNodeOpt.isPresent()) {
                UUID uuid = firstNodeOpt.get().nodeId();

                executeTaskByNameOnNode(client,
                    PersistenceCleaningTask.class.getName(),
                    convertArguments(cleaningArgs),
                    uuid,
                    clientCfg
                    );

            }
            else
                logger.warning("No nodes found in topology, command won't be executed.");
        }
        catch (Throwable t) {
            logger.severe("Failed to execute command");
            logger.severe(CommandLogger.errorMessage(t));

            throw t;
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public PersistenceCleaningArguments arg() {
        return cleaningArgs;
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger logger) {

    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        if (!argIter.hasNextSubArg()) {
            cleaningArgs = new PersistenceCleaningArguments(PersistenceCleaningSubcommands.INFO);

            return;
        }
        else
            System.out.println("-->>-->> [" + Thread.currentThread().getName() + "] something found");
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return PERSISTENCE_CLEANING.toCommandName();
    }

    private PersistenceCleaningTaskArg convertArguments(PersistenceCleaningArguments args) {
        PersistenceCleaningTaskArg taskArgs = new PersistenceCleaningTaskArg(args.subcommand().cleaningOperation());

        return taskArgs;
    }
}
