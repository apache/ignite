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

package org.apache.ignite.internal.commandline.dr.subcommands;

import java.util.logging.Logger;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.dr.DrSubCommandsList;
import org.apache.ignite.internal.visor.dr.VisorDrCacheTaskArgs;
import org.apache.ignite.internal.visor.dr.VisorDrCacheTaskResult;

import static org.apache.ignite.internal.commandline.CommandHandler.DELIM;

/** */
public class DrFullStateTransferCommand extends
    DrAbstractRemoteSubCommand<VisorDrCacheTaskArgs, VisorDrCacheTaskResult, DrCacheCommand.DrCacheArguments>
{
    /** {@inheritDoc} */
    @Override protected String visorTaskName() {
        return "org.gridgain.grid.internal.visor.dr.console.VisorDrCacheTask";
    }

    /** {@inheritDoc} */
    @Override public DrCacheCommand.DrCacheArguments parseArguments0(CommandArgIterator argIter) {
        return new DrCacheCommand.DrCacheArguments(
            ".*",
            false,
            false,
            DrCacheCommand.CacheFilter.SENDING,
            DrCacheCommand.SenderGroup.ALL,
            null,
            DrCacheCommand.Action.FULL_STATE_TRANSFER,
            (byte)0
        );
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt() {
        return "Warning: this command will execute full state transfer for all caches. This migth take a long time.";
    }

    /** {@inheritDoc} */
    @Override protected void printResult(VisorDrCacheTaskResult res, Logger log) {
        log.info("Data Center ID: " + res.getDataCenterId());

        log.info(DELIM);

        if (res.getDataCenterId() == 0) {
            log.info("Data Replication state: is not configured.");

            return;
        }

        if (res.getCacheNames().isEmpty())
            log.info("No suitable caches found for transfer.");
        else if (res.getResultMessages().isEmpty())
            log.info("Full state transfer command completed successfully for caches " + res.getCacheNames());
        else {
            for (String msg : res.getResultMessages())
                log.info(msg);
        }
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return DrSubCommandsList.FULL_STATE_TRANSFER.text();
    }
}
