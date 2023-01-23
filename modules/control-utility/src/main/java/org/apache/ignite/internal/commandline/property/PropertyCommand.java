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

package org.apache.ignite.internal.commandline.property;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.AbstractCommand;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedChangeableProperty;

import static org.apache.ignite.internal.commandline.CommandList.PROPERTY;
import static org.apache.ignite.internal.commandline.property.PropertySubCommandsList.GET;
import static org.apache.ignite.internal.commandline.property.PropertySubCommandsList.HELP;
import static org.apache.ignite.internal.commandline.property.PropertySubCommandsList.LIST;
import static org.apache.ignite.internal.commandline.property.PropertySubCommandsList.SET;

/**
 * Command to manage distributed properties (see {@link DistributedChangeableProperty})
 */
public class PropertyCommand extends AbstractCommand<Object> {
    /**
     *
     */
    private Command<?> delegate;

    /** {@inheritDoc} */
    @Override public void printUsage(IgniteLogger log) {
        usage(log, "Print property command help:",
            PROPERTY,
            HELP.toString()
        );

        usage(log, "Print list of available properties:",
            PROPERTY,
            LIST.toString()
        );

        usage(log, "Get the property value:",
            PROPERTY,
            GET.toString(),
            PropertyArgs.NAME,
            "<property_name>");

        usage(log, "Set the property value:",
            PROPERTY,
            SET.toString(),
            PropertyArgs.NAME,
            "<property_name>",
            PropertyArgs.VAL,
            "<property_value>");
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return PROPERTY.toCommandName();
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        PropertySubCommandsList subcommand = PropertySubCommandsList.parse(argIter.nextArg("Expected property action."));

        if (subcommand == null)
            throw new IllegalArgumentException("Expected correct property action.");

        delegate = subcommand.command();

        delegate.parseArguments(argIter);
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt() {
        return delegate != null ? delegate.confirmationPrompt() : null;
    }

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, IgniteLogger log) throws Exception {
        return delegate.execute(clientCfg, log);
    }

    /** {@inheritDoc} */
    @Override public Object arg() {
        return delegate.arg();
    }
}
