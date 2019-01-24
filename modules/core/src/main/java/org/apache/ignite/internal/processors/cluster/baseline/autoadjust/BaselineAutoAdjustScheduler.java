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

package org.apache.ignite.internal.processors.cluster.baseline.autoadjust;

import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;

/**
 * This class able to add task of set baseline with timeout to queue. In one time only one task can be in queue. Every
 * next queuing task evicted previous one.
 */
class BaselineAutoAdjustScheduler {
    /** Timeout processor. */
    private final GridTimeoutProcessor timeoutProcessor;
    /** Executor of set baseline operation. */
    private final BaselineAutoAdjustExecutor baselineAutoAdjustExecutor;
    /** Last set task for set new baseline. It needed for removing from queue. */
    private GridTimeoutObject baselineTimeoutObj;

    /**
     * @param timeoutProcessor Timeout processor.
     * @param baselineAutoAdjustExecutor Executor of set baseline operation.
     */
    public BaselineAutoAdjustScheduler(GridTimeoutProcessor timeoutProcessor, BaselineAutoAdjustExecutor baselineAutoAdjustExecutor) {
        this.timeoutProcessor = timeoutProcessor;
        this.baselineAutoAdjustExecutor = baselineAutoAdjustExecutor;
    }

    /**
     * Adding task to queue with delay and remove previous one.
     *
     * @param baselineAutoAdjustData Data for changing baseline.
     * @param delay Delay after which set baseline should be started.
     */
    public synchronized void schedule(BaselineAutoAdjustData baselineAutoAdjustData, long delay) {
        if (baselineTimeoutObj != null)
            timeoutProcessor.removeTimeoutObject(baselineTimeoutObj);

        timeoutProcessor.addTimeoutObject(
            baselineTimeoutObj = new GridTimeoutObjectAdapter(delay) {
                @Override public void onTimeout() {
                    baselineAutoAdjustExecutor.execute(baselineAutoAdjustData);
                }
            }
        );
    }
}
