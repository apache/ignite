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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.ignite.tensorflow.core.longrunning.LongRunningProcess;
import org.apache.ignite.tensorflow.core.util.CustomizableThreadFactory;

/**
 * Task that starts long running processes by their specifications.
 */
public class LongRunningProcessStartTask extends LongRunningProcessTask<List<UUID>> {
    /** */
    private static final long serialVersionUID = -3934183044853083034L;

    /** Process specifications. */
    private final List<LongRunningProcess> processes;

    /**
     * Constructs a new instance of start task.
     *
     * @param processes Process specifications.
     */
    public LongRunningProcessStartTask(List<LongRunningProcess> processes) {
        assert processes != null : "Processes should not be null";

        this.processes = processes;
    }

    /** {@inheritDoc} */
    @Override public List<UUID> call() {
        ArrayList<UUID> res = new ArrayList<>();

        try {
            for (LongRunningProcess proc : processes) {
                Future<?> fut = runTask(proc.getTask());

                UUID procId = saveProcMetadata(fut);

                res.add(procId);
            }
        }
        catch (Exception e) {
            // All-or-nothing strategy. In case of exception already started processes will be stopped.
            stopAllProcessesAndClearMetadata(res);

            throw e;
        }

        return res;
    }

    /**
     * Executes the task in a separate thread.
     *
     * @param task Task to be executed.
     * @return Future that allows to interrupt or get the status of the task.
     */
    private Future<?> runTask(Runnable task) {
        return Executors
            .newSingleThreadExecutor(new CustomizableThreadFactory("tf-long-running", true))
            .submit(task);
    }

    /**
     * Saves process metadata into the local metadata storage.
     *
     * @param fut Future that allows to interrupt or get the status of the task.
     * @return Process identifier.
     */
    private UUID saveProcMetadata(Future<?> fut) {
        Map<UUID, Future<?>> metadataStorage = getMetadataStorage();

        UUID procId = UUID.randomUUID();

        metadataStorage.put(procId, fut);

        return procId;
    }

    /**
     * Stop all processes by their identifiers and removes them from the metadata storage.
     *
     * @param procIds Process identifiers.
     */
    private void stopAllProcessesAndClearMetadata(List<UUID> procIds) {
        Map<UUID, Future<?>> metadataStorage = getMetadataStorage();

        for (UUID procId : procIds) {
            Future<?> fut = metadataStorage.remove(procId);
            fut.cancel(true);
        }
    }
}
