/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
