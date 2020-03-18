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

package org.apache.ignite.internal.processors.cache.checker.processor;

import java.util.Objects;

/**
 * This listener allows tracking the lifecycle stages of a workload.
 */
@FunctionalInterface
public interface ReconciliationEventListener {
    /**
     * Workload lifecycle stages.
     */
    enum WorkLoadStage {
        /** Workload is scheduled for processing. */
        SCHEDULED,

        /** Workload is ready to be processed. */
        BEFORE_PROCESSING,

        /** Workload has been processed and the processing result is ready to be used. */
        READY,

        /** Processing of the workload is completed. */
        FINISHED
    }

    /**
     * Callbeck for processing the given {@code stage} event of the correcponding {@code workload}.
     *
     * @param stage Workload lifecycle stage.
     * @param workload Workload.
     */
    void onEvent(WorkLoadStage stage, PipelineWorkload workload);

    /**
     * Returns a composed ReconciliationEventListener that performs, in sequence, this
     * operation followed by the {@code after} operation. If performing either
     * operation throws an exception, it is relayed to the caller of the
     * composed operation.  If performing this operation throws an exception,
     * the {@code after} operation will not be performed.
     *
     * @param after Listener to be called after this listener.
     * @return Composed {@code ReconciliationEventListener} that performs in sequence this
     *      listener followed by the {@code after} listener.
     * @throws NullPointerException If {@code after} is null.
     */
    default ReconciliationEventListener andThen(ReconciliationEventListener after) {
        Objects.requireNonNull(after);

        return (stage, workload) -> {
            onEvent(stage, workload);

            after.onEvent(stage, workload);
        };
    }
}
