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

package org.apache.ignite.tensorflow.core.longrunning;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterGroupEmptyException;
import org.apache.ignite.tensorflow.core.ProcessManager;
import org.apache.ignite.tensorflow.core.longrunning.task.LongRunningProcessClearTask;
import org.apache.ignite.tensorflow.core.longrunning.task.LongRunningProcessPingTask;
import org.apache.ignite.tensorflow.core.longrunning.task.LongRunningProcessStartTask;
import org.apache.ignite.tensorflow.core.longrunning.task.LongRunningProcessStopTask;
import org.apache.ignite.tensorflow.core.longrunning.task.LongRunningProcessTask;
import org.apache.ignite.tensorflow.core.longrunning.task.util.LongRunningProcessStatus;

/**
 * Long running process manager that allows to start, stop and make other actions with long running processes.
 */
public class LongRunningProcessManager implements ProcessManager<LongRunningProcess> {
    /** */
    private static final long serialVersionUID = 1151455641358063287L;

    /** Ignite instance supplier. */
    private final Supplier<Ignite> igniteSupplier;

    /**
     * Constructs a new instance of long running process manager.
     *
     * @param igniteSupplier Ignite instance supplier.
     * @param <T> Type of serializable supplier.
     */
    public <T extends Supplier<Ignite> & Serializable> LongRunningProcessManager(T igniteSupplier) {
        assert igniteSupplier != null : "Ignite supplier should not be null";

        this.igniteSupplier = igniteSupplier;
    }

    /** {@inheritDoc} */
    @Override public Map<UUID, List<UUID>> start(List<LongRunningProcess> specifications) {
        return call(groupByNodeId(specifications), LongRunningProcessStartTask::new, this::rollbackStartTask, false);
    }

    /** {@inheritDoc} */
    @Override public Map<UUID, List<LongRunningProcessStatus>> ping(Map<UUID, List<UUID>> procIds) {
        return call(procIds, LongRunningProcessPingTask::new, this::rollbackNothing, false);
    }

    /** {@inheritDoc} */
    @Override public Map<UUID, List<LongRunningProcessStatus>> stop(Map<UUID, List<UUID>> procIds, boolean clear) {
        return call(procIds, params -> new LongRunningProcessStopTask(params, clear), this::rollbackNothing, true);
    }

    /** {@inheritDoc} */
    @Override public Map<UUID, List<LongRunningProcessStatus>> clear(Map<UUID, List<UUID>> procIds) {
        return call(procIds, LongRunningProcessClearTask::new, this::rollbackNothing, true);
    }

    /**
     * Sends the specified tasks to the cluster to be executed.
     *
     * @param params Parameters needed to create tasks.
     * @param taskSupplier Supplier that defines how to create tasks.
     * @param rollback Rollback procedure.
     * @param onlyIfNodeExists If node doesn't exist the correspondent task will be ignored.
     * @param <T> Type of params needed to create tasks.
     * @param <E> Type of returned by task value.
     * @return Map of node identifier as a key and list task results as a value.
     */
    private <T, E> Map<UUID, List<E>> call(Map<UUID, List<T>> params,
        Function<List<T>, LongRunningProcessTask<List<E>>> taskSupplier, Consumer<Map<UUID, List<E>>> rollback,
        boolean onlyIfNodeExists) {
        Map<UUID, List<E>> res = new HashMap<>();

        try {
            for (UUID nodeId : params.keySet()) {
                List<T> nodeProcesses = params.get(nodeId);
                LongRunningProcessTask<List<E>> task = taskSupplier.apply(nodeProcesses);

                Ignite ignite = igniteSupplier.get();
                ClusterGroup clusterGrp = ignite.cluster().forNodeId(nodeId);

                try {
                    List<E> nodeRes = ignite.compute(clusterGrp).call(task);
                    res.put(nodeId, nodeRes);
                }
                catch (ClusterGroupEmptyException e) {
                    if (!onlyIfNodeExists)
                        throw e;
                }
            }
        }
        catch (Exception e) {
            // All-or-nothing strategy. In case of exception already processed tasks should be rolled back.
            rollback.accept(res);

            throw e;
        }

        return res;
    }

    /**
     * Groups the given process specifications by node identifier.
     *
     * @param specifications Process specifications.
     * @return Map of node identifier as a key and list of process specifications as a value.
     */
    private Map<UUID, List<LongRunningProcess>> groupByNodeId(List<LongRunningProcess> specifications) {
        Map<UUID, List<LongRunningProcess>> res = new HashMap<>();

        for (LongRunningProcess spec : specifications) {
            UUID nodeId = spec.getNodeId();

            List<LongRunningProcess> nodeSpecs = res.get(nodeId);

            if (nodeSpecs == null) {
                nodeSpecs = new ArrayList<>();
                nodeSpecs.add(spec);
                res.put(nodeId, nodeSpecs);
            }
            else
                nodeSpecs.add(spec);
        }

        return res;
    }

    /**
     * Rolls back start task successfully applied earlier.
     *
     * @param procIds Process identifiers.
     */
    private void rollbackStartTask(Map<UUID, List<UUID>> procIds) {
        stop(procIds, true);
    }

    /**
     * Rolls back nothing. Ping, stop and clear tasks cannot be rolled back, so it's the only one available strategy
     * for these tasks.
     */
    private void rollbackNothing(Map<UUID, List<LongRunningProcessStatus>> processes) {
        // Do nothing.
    }
}
