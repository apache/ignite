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

import java.util.Collections;
import java.util.Map;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Additional commands provider for control utility.
 */
public class CommandsProviderExtImpl implements CommandsProvider {
    /** */
    public static final Command<?> TEST_COMMAND = new TestCommand();

    /** */
    public static final String TEST_COMMAND_OUTPUT = "Test command executed";

    /** */
    public static final String TEST_COMMAND_USAGE = "Test command usage.";

    /** */
    public static final String TEST_COMMAND_ARG = "test-print";

    /** {@inheritDoc} */
    @Override public Map<String, Command<?>> commands() {
        return Collections.singletonMap(TEST_COMMAND.name(), TEST_COMMAND);
    }

    /** */
    public static class TestCommand implements Command<Object> {
        /** */
        private String arg;

        /** {@inheritDoc} */
        @Override public Object execute(GridClientConfiguration clientCfg, IgniteLogger log, boolean verbose) {
            log.info(TEST_COMMAND_OUTPUT + ": " + arg);

            return null;
        }

        /** {@inheritDoc} */
        @Override public void parseArguments(CommandArgIterator argIter) {
            String cmdArg = argIter.nextArg("Required 1-st argument");

            if (TEST_COMMAND_ARG.equals(cmdArg))
                arg = argIter.nextArg("Required 2-nd argument");
            else
                throw new IllegalArgumentException("Invalid argument \"" + cmdArg + "\".");

            if (argIter.hasNextSubArg()) {
                throw new IllegalArgumentException(
                    "Invalid argument \"" + argIter.peekNextArg() + "\", no more arguments is expected.");
            }
        }

        /** {@inheritDoc} */
        @Override public Object arg() {
            return arg;
        }

        /** {@inheritDoc} */
        @Override public void printUsage(IgniteLogger log) {
            usage(log, TEST_COMMAND_USAGE, TEST_COMMAND.name(), F.asMap("value", "Value to print"),
                TEST_COMMAND_ARG, "value");
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return "--test-command";
        }
    }
}
