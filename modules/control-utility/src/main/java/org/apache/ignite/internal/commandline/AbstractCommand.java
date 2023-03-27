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

import java.util.Map;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.cache.CacheSubcommands;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.ignite.internal.commandline.CommandList.CACHE;
import static org.apache.ignite.internal.commandline.CommandLogger.DOUBLE_INDENT;
import static org.apache.ignite.internal.commandline.CommandLogger.INDENT;

/**
 * Abstract class for control.sh commands, that support verbose mode.
 */
public abstract class AbstractCommand<T> implements Command<T> {
    /** Use verbose mode or not. */
    protected boolean verbose;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, IgniteLogger log, boolean verbose) throws Exception {
        this.verbose = verbose;
        return execute(clientCfg, log);
    }

    /**
     * Print cache command usage with default indention.
     *
     * @param logger Logger to use.
     * @param cmd Cache command.
     * @param description Command description.
     * @param paramsDesc Parameter desciptors.
     * @param args Cache command arguments.
     */
    protected void usageCache(
        IgniteLogger logger,
        CacheSubcommands cmd,
        String description,
        Map<String, String> paramsDesc,
        String... args
    ) {
        logger.info("");
        logger.info(INDENT + CommandLogger.join(" ", CACHE, cmd, CommandLogger.join(" ", args)));
        logger.info(DOUBLE_INDENT + description);

        if (!F.isEmpty(paramsDesc)) {
            logger.info("");
            logger.info(DOUBLE_INDENT + "Parameters:");

            usageParams(paramsDesc, DOUBLE_INDENT + INDENT, logger);
        }
    }
}
