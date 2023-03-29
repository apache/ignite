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

package org.apache.ignite.internal.commandline.cdc;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.commandline.AbstractCommand;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.cdc.VisorCdcFlushCachesTask;
import org.apache.ignite.internal.visor.cdc.VisorCdcFlushCachesTaskArg;

import static org.apache.ignite.internal.commandline.CommandList.CDC;

/**
 * The command to flush caches. Iterates over caches and writes data entries to the WAL to get captured by CDC.
 */
public class FlushCachesCommand extends AbstractCommand<Object> {
    /** Command to flush caches. */
    public static final String FLUSH_CACHES = "flush_caches";

    /** */
    public static final String CACHES = "--caches";

    /** */
    private VisorCdcFlushCachesTaskArg arg;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, IgniteLogger log) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            Collection<UUID> nodeIds = client.compute().nodes(node -> !node.isClient()).stream()
                .map(GridClientNode::nodeId).collect(Collectors.toSet());

            client.compute().execute(VisorCdcFlushCachesTask.class.getName(),
                new VisorTaskArgument<>(nodeIds, arg, false));

            String res = "CDC flush caches finished.";

            log.info(res);

            return res;
        }
        catch (Throwable e) {
            log.error("Failed to perform operation.");
            log.error(CommandLogger.errorMessage(e));

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        Set<String> caches = null;

        while (argIter.hasNextSubArg()) {
            String opt = argIter.nextArg("Failed to read command argument.");

            if (CACHES.equalsIgnoreCase(opt)) {
                if (caches != null)
                    throw new IllegalArgumentException(CACHES + " arg specified twice.");

                caches = argIter.nextStringSet("comma-separated list of cache names.");
            }
        }

        if (F.isEmpty(caches))
            throw new IllegalArgumentException("At least one cache name should be specified.");

        arg = new VisorCdcFlushCachesTaskArg(caches);
    }

    /** {@inheritDoc} */
    @Override public void printUsage(IgniteLogger logger) {
        Map<String, String> params = new LinkedHashMap<>();

        params.put(CACHES + " cache1,...,cacheN", "specifies a comma-separated list of cache names to be flushed.");

        usage(logger,
            "Flush cache data entries. Iterates over caches and writes data entries to the WAL to get captured by CDC:",
            CDC, params, FLUSH_CACHES, CACHES, "cache1,...,cacheN");
    }

    /** {@inheritDoc} */
    @Override public Object arg() {
        return arg;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return FLUSH_CACHES;
    }

    /** {@inheritDoc} */
    @Override public boolean experimental() {
        return true;
    }
}
