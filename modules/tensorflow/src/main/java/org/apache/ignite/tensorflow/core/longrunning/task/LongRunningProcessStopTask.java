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

import org.apache.ignite.tensorflow.core.longrunning.task.util.LongRunningProcessStatus;
import org.apache.ignite.tensorflow.core.longrunning.task.util.LongRunningProcessState;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;

/**
 * Task that stops long running processes.
 */
public class LongRunningProcessStopTask extends LongRunningProcessTask<List<LongRunningProcessStatus>> {
    /** */
    private static final long serialVersionUID = -5552468435820611170L;

    /** Process identifiers. */
    private final List<UUID> procIds;

    /** Flag that defines that metadata should be removed immediately. */
    private final boolean clear;

    /**
     * Constructs a new instance of stop task.
     *
     * @param procIds Process identifiers.
     * @param clear Flag that defines that metadata should be removed immediately.
     */
    public LongRunningProcessStopTask(List<UUID> procIds, boolean clear) {
        assert procIds != null : "Process identifiers should not be null";

        this.procIds = procIds;
        this.clear = clear;
    }

    /** {@inheritDoc} */
    @Override public List<LongRunningProcessStatus> call() {
        ArrayList<LongRunningProcessStatus> res = new ArrayList<>();

        for (UUID prodId : procIds)
            res.add(stopProcess(prodId));

        // All-or-nothing strategy. Processes will be removed only if all processes can be removed.
        if (clear)
            removeProcessesFromMetadataStorage();

        return res;
    }

    /**
     * Stop process by process identifier.
     *
     * @param procId Process identifier.
     * @return Process status after stop.
     */
    private LongRunningProcessStatus stopProcess(UUID procId) {
        Map<UUID, Future<?>> metadataStorage = getMetadataStorage();

        Future<?> fut = metadataStorage.get(procId);

        if (fut == null)
            return new LongRunningProcessStatus(LongRunningProcessState.NOT_FOUND);

        try {
            fut.cancel(true);
            fut.get();
        }
        catch (Exception e) {
            return new LongRunningProcessStatus(LongRunningProcessState.DONE, e);
        }

        return new LongRunningProcessStatus(LongRunningProcessState.DONE);
    }

    /**
     * Removes processes from metadata storage.
     */
    private void removeProcessesFromMetadataStorage() {
        Map<UUID, Future<?>> metadataStorage = getMetadataStorage();

        for (UUID procId : procIds)
            metadataStorage.remove(procId);
    }
}
