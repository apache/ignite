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
