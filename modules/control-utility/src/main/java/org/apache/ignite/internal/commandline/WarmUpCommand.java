/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.commandline;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.client.GridClientBeforeNodeStart;
import org.apache.ignite.internal.client.GridClientDisconnectedException;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.commandline.argument.CommandArg;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;

import static java.util.Objects.isNull;
import static org.apache.ignite.internal.commandline.CommandList.WARM_UP;

/**
 * Command for interacting with warm-up.
 */
public class WarmUpCommand extends AbstractCommand<Void> {
    /** {@inheritDoc} */
    @Override public void printUsage(IgniteLogger logger) {
        usage(logger, "Stop warm-up:", WARM_UP, WarmUpCommandArg.STOP.argName());
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return CommandList.WARM_UP.toCommandName();
    }

    /** {@inheritDoc} */
    @Override public Void arg() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        boolean stop = false;

        while (true) {
            String nextArg = argIter.peekNextArg();

            if (nextArg == null)
                break;

            WarmUpCommandArg arg = CommandArgUtils.of(nextArg, WarmUpCommandArg.class);

            if (isNull(arg))
                break;

            switch (arg) {
                case STOP:
                    argIter.nextArg("");

                    stop = true;
                    break;

                default:
                    throw new AssertionError();
            }
        }

        if (!stop)
            throw new IllegalArgumentException(WarmUpCommandArg.STOP.argName() + " argument is missing.");
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt() {
        return "Warning: command will stop warm-up.";
    }

    /** {@inheritDoc} */
    @Override public Object execute(ClientConfiguration clientCfg, IgniteLogger log) throws Exception {
        try (GridClientBeforeNodeStart client = startClientBeforeNodeStart(clientCfg)) {
            client.beforeStartState().stopWarmUp();
        }
        catch (GridClientDisconnectedException e) {
            throw new GridClientException(e.getCause());
        }

        return true;
    }

    /**
     * Method to create thin client for communication with node before it starts.
     * If node has already started, there will be an error.
     *
     * @param clientCfg Thin client configuration.
     * @return Grid thin client instance which is already connected to node before it starts.
     * @throws Exception If error occur.
     */
    public static GridClientBeforeNodeStart startClientBeforeNodeStart(
        ClientConfiguration clientCfg
    ) throws Exception {
        GridClientBeforeNodeStart client = null; //GridClientFactory.startBeforeNodeStart(clientCfg);

        // If connection is unsuccessful, fail before doing any operations:
        if (!client.connected()) {
            GridClientException lastErr = client.checkLastError();

            try {
                client.close();
            }
            catch (Throwable e) {
                lastErr.addSuppressed(e);
            }

            throw lastErr;
        }

        return client;
    }

    /**
     * Warm-up command arguments name.
     */
    private enum WarmUpCommandArg implements CommandArg {
        /** Stop warm-up argument. */
        STOP("--stop");

        /** Option name. */
        private final String name;

        /**
         * Constructor.
         *
         * @param name Argument name.
         */
        WarmUpCommandArg(String name) {
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public String argName() {
            return name;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return name;
        }
    }
}
