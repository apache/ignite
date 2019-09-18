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

package org.apache.ignite.internal.commandline.dr;

import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.dr.subcommands.DrCacheCommand;
import org.apache.ignite.internal.commandline.dr.subcommands.DrNodeCommand;
import org.apache.ignite.internal.commandline.dr.subcommands.DrStateCommand;
import org.apache.ignite.internal.commandline.dr.subcommands.DrTopologyCommand;

import static org.apache.ignite.internal.commandline.Command.usage;
import static org.apache.ignite.internal.commandline.CommandList.DATA_CENTER_REPLICATION;
import static org.apache.ignite.internal.commandline.CommandLogger.join;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_AUTO_CONFIRMATION;
import static org.apache.ignite.internal.commandline.dr.DrSubCommandsList.CACHE;
import static org.apache.ignite.internal.commandline.dr.DrSubCommandsList.FULL_STATE_TRANSFER;
import static org.apache.ignite.internal.commandline.dr.DrSubCommandsList.HELP;
import static org.apache.ignite.internal.commandline.dr.DrSubCommandsList.NODE;
import static org.apache.ignite.internal.commandline.dr.DrSubCommandsList.PAUSE;
import static org.apache.ignite.internal.commandline.dr.DrSubCommandsList.RESUME;
import static org.apache.ignite.internal.commandline.dr.DrSubCommandsList.STATE;
import static org.apache.ignite.internal.commandline.dr.DrSubCommandsList.TOPOLOGY;

/** */
public class DrCommand implements Command<Object> {
    /** */
    private Command<?> delegate;

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        usage(log, "Print data center replication command help:",
            DATA_CENTER_REPLICATION,
            HELP.toString()
        );

        usage(log, "Print state of data center replication:",
            DATA_CENTER_REPLICATION,
            STATE.toString(),
            optional(DrStateCommand.VERBOSE_PARAM)
        );

        usage(log, "Print topology of the cluster with the data center replication related details:",
            DATA_CENTER_REPLICATION,
            TOPOLOGY.toString(),
            optional(DrTopologyCommand.SENDER_HUBS_PARAM),
            optional(DrTopologyCommand.RECEIVER_HUBS_PARAM),
            optional(DrTopologyCommand.DATA_NODES_PARAM),
            optional(DrTopologyCommand.OTHER_NODES_PARAM)
        );

        usage(log, "Print node specific data center replication related details and clear node's DR store:",
            DATA_CENTER_REPLICATION,
            NODE.toString(),
            "<nodeId>",
            optional(DrNodeCommand.CONFIG_PARAM),
            optional(DrNodeCommand.METRICS_PARAM),
            optional(DrNodeCommand.CLEAR_STORE_PARAM),
            optional(CMD_AUTO_CONFIRMATION)
        );

        usage(log, "Print cache specific data center replication related details about caches and maybe change replication state on them:",
            DATA_CENTER_REPLICATION,
            CACHE.toString(),
            "<regExp>",
            optional(DrCacheCommand.CONFIG_PARAM),
            optional(DrCacheCommand.METRICS_PARAM),
            optional(DrCacheCommand.CACHE_FILTER_PARAM, join("|", DrCacheCommand.CacheFilter.values())),
            optional(DrCacheCommand.SENDER_GROUP_PARAM, "<groupName>|" + join("|", DrCacheCommand.SenderGroup.values())),
            optional(DrCacheCommand.ACTION_PARAM, join("|", DrCacheCommand.Action.values())),
            optional(CMD_AUTO_CONFIRMATION)
        );

        usage(log, "Execute full state transfer on all caches in cluster if data center replication is configured:",
            DATA_CENTER_REPLICATION,
            FULL_STATE_TRANSFER.toString(),
            optional(CMD_AUTO_CONFIRMATION)
        );

        usage(log, "Stop data center replication on all caches in cluster:",
            DATA_CENTER_REPLICATION,
            PAUSE.toString(),
            "<remoteDataCenterId>",
            optional(CMD_AUTO_CONFIRMATION)
        );

        usage(log, "Start data center replication on all caches in cluster:",
            DATA_CENTER_REPLICATION,
            RESUME.toString(),
            "<remoteDataCenterId>",
            optional(CMD_AUTO_CONFIRMATION)
        );
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return DATA_CENTER_REPLICATION.toCommandName();
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        DrSubCommandsList subcommand = DrSubCommandsList.parse(argIter.nextArg("Expected dr action."));

        if (subcommand == null)
            throw new IllegalArgumentException("Expected correct dr action.");

        delegate = subcommand.command();

        delegate.parseArguments(argIter);
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt() {
        return delegate != null ? delegate.confirmationPrompt() : null;
    }

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        return delegate.execute(clientCfg, log);
    }

    /** {@inheritDoc} */
    @Override public Object arg() {
        return delegate.arg();
    }
}
