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
public class DrPauseCommand extends
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
            DrCacheCommand.CacheFilter.ALL,
            DrCacheCommand.SenderGroup.ALL,
            null,
            DrCacheCommand.Action.STOP,
            argIter.nextByteArg("remoteDataCenterId")
        );
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt() {
        return "Warning: this command will pause data center replication for all caches.";
    }

    /** {@inheritDoc} */
    @Override protected void printResult(VisorDrCacheTaskResult res, Logger log) {
        log.info("Data Center ID: " + res.getDataCenterId());

        log.info(DELIM);

        if (res.getDataCenterId() == 0) {
            log.info("Data Replication state: is not configured.");

            return;
        }

        for (String msg : res.getResultMessages())
            log.info(msg);
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return DrSubCommandsList.PAUSE.text();
    }
}
