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

package org.apache.ignite.internal.processors.hadoop;

import java.io.Serializable;
import java.util.Collection;
import java.util.UUID;
import org.jetbrains.annotations.Nullable;

/**
 * Map-reduce job execution plan.
 */
public interface HadoopMapReducePlan extends Serializable {
    /**
     * Gets collection of file blocks for which mappers should be executed.
     *
     * @param nodeId Node ID to check.
     * @return Collection of file blocks or {@code null} if no mappers should be executed on given node.
     */
    @Nullable public Collection<HadoopInputSplit> mappers(UUID nodeId);

    /**
     * Gets reducer IDs that should be started on given node.
     *
     * @param nodeId Node ID to check.
     * @return Array of reducer IDs.
     */
    @Nullable public int[] reducers(UUID nodeId);

    /**
     * Gets collection of all node IDs involved in map part of job execution.
     *
     * @return Collection of node IDs.
     */
    public Collection<UUID> mapperNodeIds();

    /**
     * Gets collection of all node IDs involved in reduce part of job execution.
     *
     * @return Collection of node IDs.
     */
    public Collection<UUID> reducerNodeIds();

    /**
     * Gets overall number of mappers for the job.
     *
     * @return Number of mappers.
     */
    public int mappers();

    /**
     * Gets overall number of reducers for the job.
     *
     * @return Number of reducers.
     */
    public int reducers();

    /**
     * Gets node ID for reducer.
     *
     * @param reducer Reducer.
     * @return Node ID.
     */
    public UUID nodeForReducer(int reducer);
}