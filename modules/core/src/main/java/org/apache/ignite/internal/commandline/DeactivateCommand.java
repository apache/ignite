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

import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientClusterState;
import org.apache.ignite.internal.client.GridClientConfiguration;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.processors.task.GridInternal;
import java.util.Collections;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.Nullable;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.compute.ComputeTask;
import com.sun.istack.internal.NotNull;
import java.util.function.Function;

import static org.apache.ignite.internal.commandline.CommandList.DEACTIVATE;
import static org.apache.ignite.internal.commandline.CommandList.SET_STATE;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.CommonArgParser.CMD_AUTO_CONFIRMATION;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_REUSE_MEMORY_ON_DEACTIVATE;

/**
 * Command to deactivate cluster.
 * @deprecated Use {@link ClusterStateChangeCommand} instead.
 */
@Deprecated
public class DeactivateCommand implements Command<Void> {
    /** Cluster name. */
    private String clusterName;

    /** Force cluster deactivation even it might have in-mem caches. */
    private boolean force;

    /**
     * Checks if resonable to deactivate without flag 'force'.
     *
     * @param taskLauncher Computation task launcher. The task has no params, just need to be launched for its result.
     * @return Empty (not-null) message if cluster is ready. Or warning text telling why deactivation is not advised.
     */
    @NotNull public static String isClusterReadyForDeactivation(
        Function<Class<? extends ComputeTask<VisorTaskArgument, Boolean>>, Boolean> taskLauncher) {
        if (!IgniteSystemProperties.getBoolean(IGNITE_REUSE_MEMORY_ON_DEACTIVATE)
            && taskLauncher.apply(FindNotPersistentCachesTask.class)) {
            return "The cluster has at least one cache configured without persistense. " +
                "During deactivation all data from these caches will be erased!";
        }
        return "";
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        Command.usage(log, "Deactivate cluster (deprecated. Use " + SET_STATE.toString() + " instead):", DEACTIVATE, optional(CMD_AUTO_CONFIRMATION, "--force"));
    }

    /** {@inheritDoc} */
    @Override public void prepareConfirmation(GridClientConfiguration clientCfg) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            clusterName = client.state().clusterName();
        }
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt() {
        return "Warning: the command will deactivate a cluster \"" + clusterName + "\"." +
            (IgniteSystemProperties.getBoolean(IGNITE_REUSE_MEMORY_ON_DEACTIVATE)
                ? ""
                : " Make sure there are no caches not backed with persistent storage.");
    }

    /**
     * Deactivate cluster.
     *
     * @param clientCfg Client configuration.
     * @throws Exception If failed to deactivate.
     */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        log.warning("Command deprecated. Use " + SET_STATE.toString() + " instead.");

        try (GridClient client = Command.startClient(clientCfg)) {

            //Search for in-memory-only caches. Warn of possible data loss.
            if (!force) {
                String msg = isClusterReadyForDeactivation((cls) -> {
                    try {
                        return TaskExecutor.executeTask(client, FindNotPersistentCachesTask.class, null, clientCfg);
                    }
                    catch (GridClientException e) {
                        throw new RuntimeException("Failed to launch task to check if cluster is ready for deactivation.", e);
                    }
                });
                if (!msg.isEmpty())
                    throw new IllegalStateException(msg + " Type --force to proceed.");
            }

            GridClientClusterState state = client.state();

            state.active(false);

            log.info("Cluster deactivated.");
        }
        catch (Exception e) {
            log.severe("Failed to deactivate cluster.");

            throw e;
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        if (argIter.hasNextArg()) {
            String arg = argIter.peekNextArg();
            if ("--force".equalsIgnoreCase(arg)) {
                force = true;
                argIter.nextArg("");
            }
        }
    }

    /** {@inheritDoc} */
    @Override public Void arg() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return DEACTIVATE.toCommandName();
    }

    /** Searches for any non-persistent cache. */
    private static class FindNotPersistentCachesJob extends ComputeJobAdapter {

        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        @SuppressWarnings("unchecked")
        @Override public Boolean execute() throws IgniteException {
            //Find data region to set persistent flag.
            for(String cacheName : ignite.cacheNames()){
                CacheConfiguration cacheCfg = ignite.cache(cacheName).getConfiguration(CacheConfiguration.class);

                DataRegionConfiguration regionCfg = cacheCfg.getDataRegionName() == null
                    ? ignite.configuration().getDataStorageConfiguration().getDefaultDataRegionConfiguration()
                    : null;

                if (regionCfg == null) {
                    for (DataRegionConfiguration dataRegionCfg : ignite.configuration().getDataStorageConfiguration()
                        .getDataRegionConfigurations()) {
                        if (dataRegionCfg.getName().equals(cacheCfg.getDataRegionName())) {
                            regionCfg = dataRegionCfg;
                            break;
                        }
                    }
                }

                if(!regionCfg.isPersistenceEnabled())
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
