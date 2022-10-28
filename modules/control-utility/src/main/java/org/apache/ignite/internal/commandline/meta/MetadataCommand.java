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

package org.apache.ignite.internal.commandline.meta;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.AbstractCommand;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.meta.subcommands.MetadataRemoveCommand;
import org.apache.ignite.internal.commandline.meta.subcommands.MetadataUpdateCommand;
import org.apache.ignite.internal.commandline.meta.tasks.MetadataTypeArgs;

import static org.apache.ignite.internal.commandline.CommandList.METADATA;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.meta.MetadataSubCommandsList.DETAILS;
import static org.apache.ignite.internal.commandline.meta.MetadataSubCommandsList.HELP;
import static org.apache.ignite.internal.commandline.meta.MetadataSubCommandsList.LIST;
import static org.apache.ignite.internal.commandline.meta.MetadataSubCommandsList.REMOVE;
import static org.apache.ignite.internal.commandline.meta.MetadataSubCommandsList.UPDATE;

/**
 *
 */
public class MetadataCommand extends AbstractCommand<Object> {
    /**
     *
     */
    private Command<?> delegate;

    /** {@inheritDoc} */
    @Override public void printUsage(IgniteLogger log) {
        usage(log, "Print metadata command help:",
            METADATA,
            HELP.toString()
        );

        usage(log, "Print list of binary metadata types:",
            METADATA,
            LIST.toString()
        );

        usage(log, "Print detailed info about specified binary type " +
                "(the type must be specified by type name or by type identifier):",
            METADATA,
            DETAILS.toString(),
            optional(MetadataTypeArgs.TYPE_ID, "<typeId>"),
            optional(MetadataTypeArgs.TYPE_NAME, "<typeName>")
        );

        usage(log, "Remove the metadata of the specified type " +
                "(the type must be specified by type name or by type identifier) from cluster and saves the removed " +
                "metadata to the specified file. \n" +
                "If the file name isn't specified the output file name is: '<typeId>.bin'",
            METADATA,
            REMOVE.toString(),
            optional(MetadataTypeArgs.TYPE_ID, "<typeId>"),
            optional(MetadataTypeArgs.TYPE_NAME, "<typeName>"),
            optional(MetadataRemoveCommand.OUT_FILE_NAME, "<fileName>")
        );

        usage(log, "Update cluster metadata from specified file (file name is required)",
            METADATA,
            UPDATE.toString(),
            MetadataUpdateCommand.IN_FILE_NAME, "<fileName>"
        );
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return METADATA.toCommandName();
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        MetadataSubCommandsList subcommand = MetadataSubCommandsList.parse(argIter.nextArg("Expected metadata action."));

        if (subcommand == null)
            throw new IllegalArgumentException("Expected correct metadata action.");

        delegate = subcommand.command();

        delegate.parseArguments(argIter);
    }

    /** {@inheritDoc} */
    @Override public boolean experimental() {
        return true;
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

    /**
     * @param argIter Command line arguments iterator.
     * @return Metadata type argument.
     */
    public static MetadataTypeArgs parseArgs(CommandArgIterator argIter) {
        String typeName = null;
        Integer typeId = null;

        while (argIter.hasNextSubArg() && typeName == null && typeId == null) {
            String optName = argIter.nextArg("Expecting " + MetadataTypeArgs.TYPE_NAME + " or " + MetadataTypeArgs.TYPE_ID);

            switch (optName) {
                case MetadataTypeArgs.TYPE_NAME:
                    typeName = argIter.nextArg("type name");

                    break;

                case MetadataTypeArgs.TYPE_ID:
                    typeId = argIter.nextIntArg("typeId");

                    break;
            }
        }

        if (typeName == null && typeId == null) {
            throw new IllegalArgumentException("Type to remove is not specified. " +
                "Please add one of the options: --typeName <type_name> or --typeId <type_id>");
        }

        return new MetadataTypeArgs(typeName, typeId);
    }
}
