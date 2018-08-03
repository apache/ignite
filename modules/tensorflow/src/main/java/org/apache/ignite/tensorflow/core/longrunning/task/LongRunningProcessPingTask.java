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
 * Task that pings processes to check their statuses.
 */
public class LongRunningProcessPingTask extends LongRunningProcessTask<List<LongRunningProcessStatus>> {
    /** */
    private static final long serialVersionUID = 7003289989579770395L;

    /** Process identifiers. */
    private final List<UUID> procIds;

    /**
     * Constructs a new instance of ping task.
     *
     * @param procIds Process identifiers.
     */
    public LongRunningProcessPingTask(List<UUID> procIds) {
        assert procIds != null : "Process identifiers should not be null";

        this.procIds = procIds;
    }

    /** {@inheritDoc} */
    @Override public List<LongRunningProcessStatus> call() {
        ArrayList<LongRunningProcessStatus> statuses = new ArrayList<>();

        for (UUID procId : procIds)
            statuses.add(getProcessStatus(procId));

        return statuses;
    }

    /**
     * Extracts the process status.
     *
     * @param procId Process identifier.
     * @return Process status.
     */
    private LongRunningProcessStatus getProcessStatus(UUID procId) {
        Map<UUID, Future<?>> metadataStorage = getMetadataStorage();

        Future<?> fut = metadataStorage.get(procId);

        if (fut == null)
            return new LongRunningProcessStatus(LongRunningProcessState.NOT_FOUND);

        if (!fut.isDone())
            return new LongRunningProcessStatus(LongRunningProcessState.RUNNING);

        try {
            fut.get();
        }
        catch (Exception e) {
            return new LongRunningProcessStatus(LongRunningProcessState.DONE, e);
        }

        return new LongRunningProcessStatus(LongRunningProcessState.DONE);
    }
}
