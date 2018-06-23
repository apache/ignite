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

package org.apache.ignite.tensorflow.core;

import org.apache.ignite.tensorflow.core.longrunning.task.util.LongRunningProcessStatus;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Process manager that allows to run and maintain processes in the cluster.
 *
 * @param <R> Type of task to be run.
 */
public interface ProcessManager<R> extends Serializable {
    /**
     * Starts the processes by the given specifications.
     *
     * @param specifications Process specifications.
     * @return Map of node identifier as a key and list of started process identifiers as a value.
     */
    public Map<UUID, List<UUID>> start(List<R> specifications);

    /**
     * Pings the given processes.
     *
     * @param procIds Map of node identifier as a key and list of process identifiers as a value.
     * @return Map of node identifier as a key and list of process statuses as a value.
     */
    public Map<UUID, List<LongRunningProcessStatus>> ping(Map<UUID, List<UUID>> procIds);

    /**
     * Stops the given processes.
     *
     * @param procIds Map of node identifier as a key and list of process identifiers as a value.
     * @return Map of node identifier as a key and list of process statuses as a value.
     */
    public Map<UUID, List<LongRunningProcessStatus>> stop(Map<UUID, List<UUID>> procIds, boolean clear);

    /**
     * Clears metadata of the given processes.
     *
     * @param procIds Map of node identifier as a key and list of process identifiers as a value.
     * @return Map of node identifier as a key and list of process statuses as a value.
     */
    public Map<UUID, List<LongRunningProcessStatus>> clear(Map<UUID, List<UUID>> procIds);
}
