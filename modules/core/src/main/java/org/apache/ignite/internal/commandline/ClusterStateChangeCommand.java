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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Logger;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import com.sun.istack.internal.NotNull;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.Nullable;
import org.apache.ignite.IgniteSystemProperties;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.cluster.ClusterState.ACTIVE_READ_ONLY;
import static org.apache.ignite.internal.commandline.CommandList.SET_STATE;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.CommandLogger.or;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_AUTO_CONFIRMATION;

/**
 * Command to change cluster state.
 */
public class ClusterStateChangeCommand implements Command<ClusterState> {
    /** New cluster state */
    private ClusterState state;

    /** Cluster name. */
    private String clusterName;

    /** Force cluster deactivation even it might have in-mem caches. */
    private boolean force;

    /**
     * Checks if it is resonable to deactivate cluster.
     *
     * @param taskLauncher Computation task launcher. The task has no params, just needs to be launched for its result.
     * @return Empty (not-null) message if cluster is ready. Or warning text telling why deactivation is not advised.
     */
    @NotNull public static String isClusterReadyForDeactivation(
        Function<Class<? extends ComputeTask<VisorTaskArgument, Boolean>>, Boolean> taskLauncher) {

        if (!IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_REUSE_MEMORY_ON_DEACTIVATE)
            && taskLauncher.apply(FindNotPersistentCachesTask.class)) {
            return "The cluster has at least one cache configured without persistence. " +
                "During deactivation all data from these caches will be erased!";
        }

        return "";
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        Map<String, String> params = new LinkedHashMap<>();

        params.put(ACTIVE.toString(), "Activate cluster. Cache updates are allowed.");
        params.put(INACTIVE.toString(), "Deactivate cluster.");
        params.put(ACTIVE_READ_ONLY.toString(), "Activate cluster. Cache updates are denied.");

        Command.usage(log, "Change cluster state:", SET_STATE, params, or((Object[])ClusterState.values()),
            optional("--force", CMD_AUTO_CONFIRMATION));
    }

    /** {@inheritDoc} */
    @Override public void prepareConfirmation(GridClientConfiguration clientCfg) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            clusterName = client.state().clusterName();
        }
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt() {
        String msg = "Warning: the command will change state of cluster with name \"" + clusterName + "\" to " + state + ".";

        if (state == INACTIVE && !IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_REUSE_MEMORY_ON_DEACTIVATE))
            msg += " Make sure there are no caches not backed with persistent storage.";

        return msg;
    }

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {

            //Search for in-memory-only caches. Fail if possible data loss.
            if (state == INACTIVE && !force) {
                String msg = isClusterReadyForDeactivation((cls) -> {
                    try {
                        return TaskExecutor.executeTask(client, cls, null, clientCfg);
                    }
                    catch (GridClientException e) {
                        throw new RuntimeException("Failed to launch task for checking if cluster is ready for deactivation.", e);
                    }
                });
                if (!msg.isEmpty())
                    throw new IllegalStateException(msg + " Please, add --force to deactivate cluster.");
            }

            client.state().state(state);

            log.info("Cluster state changed to " + state);

            return null;
        }
        catch (Throwable e) {
            log.info("Failed to change cluster state to " + state);

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        String s = argIter.nextArg("New cluster state not found.");

        try {
            state = ClusterState.valueOf(s.toUpperCase());
        }
        catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Can't parse new cluster state. State: " + s, e);
        }

        if (argIter.hasNextArg()) {
            String arg = argIter.peekNextArg();
            if ("--force".equalsIgnoreCase(arg)) {
                force = true;
                argIter.nextArg("");
            }
        }
    }

    /** {@inheritDoc} */
    @Override public ClusterState arg() {
        return state;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return SET_STATE.toCommandName();
    }

    /** Searches for any non-persistent cache. */
    private static class FindNotPersistentCachesJob extends ComputeJobAdapter {

        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        @SuppressWarnings("unchecked")
        @Override public Boolean execute() throws IgniteException {
            //Find data region to set persistent flag.
            for (String cacheName : ignite.cacheNames()) {
                CacheConfiguration cacheCfg = ignite.cache(cacheName).getConfiguration(CacheConfiguration.class);

                DataRegionConfiguration regionCfg = cacheCfg.getDataRegionName() == null
                    ? ignite.configuration().getDataStorageConfiguration().getDefaultDataRegionConfiguration()
                    : Stream.of(ignite.configuration().getDataStorageConfiguration()
                    .getDataRegionConfigurations()).filter(region -> region.getName().equals(cacheCfg.getDataRegionName()))
                    .findFirst().orElse(null);

                if (regionCfg == null)
                    throw new IgniteException("Failed to check if cluster is ready for deactivation. " +
                        "While searching for non-persistent caches, " +
                        "no data region found: \"" + cacheCfg.getDataRegionName() + "\" for cache \"" + cacheName + "\". " +
                        "Check configurations of memory storage and caches.");

                if (!regionCfg.isPersistenceEnabled())
                    return true;
            }

            return false;
        }
    }

    /** Searches for any non-persistent cache. */
    @GridInternal
    private static class FindNotPersistentCachesTask extends ComputeTaskSplitAdapter<VisorTaskArgument, Boolean> {

        /** Provides one job. */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, VisorTaskArgument arg) throws IgniteException {
            return Collections.singletonList(new FindNotPersistentCachesJob());
        }

        /** Only one result is expected. */
        @Nullable @Override public Boolean reduce(List<ComputeJobResult> results) throws IgniteException {
            return results.get(0).getData();
        }
    }
}
