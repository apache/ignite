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

package org.apache.ignite.tensorflow.core.longrunning.task;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.tensorflow.core.longrunning.LongRunningProcessManager;

/**
 * Task that can be executed on a cluster using the {@link LongRunningProcessManager}.
 *
 * @param <T> Type of the result.
 */
public abstract class LongRunningProcessTask<T> implements IgniteCallable<T> {
    /** */
    private static final long serialVersionUID = 2351380200758417004L;

    /** Long running process storage name. */
    private static final String LONG_RUNNING_PROCESS_STORAGE_NAME = "LONG_RUNNING_PROCESS_STORAGE";

    /**
     * Returns task metadata storage that is kept by every node in the cluster.
     *
     * @return Task metadata storage that is kept by every node in the cluster.
     */
    ConcurrentHashMap<UUID, Future<?>> getMetadataStorage() {
        Ignite ignite = Ignition.localIgnite();

        ConcurrentMap<String, ConcurrentHashMap<UUID, Future<?>>> nodeLocMap = ignite.cluster().nodeLocalMap();

        return nodeLocMap.computeIfAbsent(LONG_RUNNING_PROCESS_STORAGE_NAME, k -> new ConcurrentHashMap<>());
    }
}
