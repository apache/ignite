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

package org.apache.ignite.internal.commandline.query;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.visor.service.VisorCancelServiceTask;
import org.apache.ignite.internal.visor.service.VisorCancelServiceTaskArg;
import org.apache.ignite.mxbean.ServiceMXBean;

import static org.apache.ignite.internal.commandline.CommandList.KILL;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;
import static org.apache.ignite.internal.commandline.query.KillSubcommand.SERVICE;

/**
 * control.sh kill command.
 *
 * @see KillSubcommand
 * @see ServiceMXBean
 */
public class KillCommand implements Command<Object> {
    /** Command argument. */
    private Object taskArgs;

    /** Task name. */
    private String taskName;

    /** Subcommand. */
    private KillSubcommand cmd;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger logger) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            Object res = executeTaskByNameOnNode(
                client,
                taskName,
                taskArgs,
                null,
                clientCfg
            );

            switch (cmd) {
                case SERVICE:
                    if (!(boolean)res)
                        throw new RuntimeException("Service not found or can't be canceled.");

                    break;
            }

            return res;
        }
        catch (Throwable e) {
            logger.severe("Failed to perform operation.");
            logger.severe(CommandLogger.errorMessage(e));

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public Object arg() {
        return taskArgs;
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        try {
            cmd = KillSubcommand.valueOf(argIter.nextArg("Expected type of resource to kill."));
        }
        catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Expected type of resource to kill.");
        }

        switch (cmd) {
            case SERVICE:
                taskArgs = new VisorCancelServiceTaskArg(argIter.nextArg("Expected service name."));

                taskName = VisorCancelServiceTask.class.getName();

                break;
        }
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger logger) {
        Map<String, String> params = new HashMap<>();

        params.put("name", "Service name.");

        Command.usage(logger, "Kill service by name:", KILL, params, SERVICE.toString(), "name");
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return KILL.toCommandName();
    }
}
