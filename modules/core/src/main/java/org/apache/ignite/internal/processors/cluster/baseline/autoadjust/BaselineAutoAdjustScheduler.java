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

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_BASELINE_AUTO_ADJUST_LOG_INTERVAL;
import static org.apache.ignite.IgniteSystemProperties.getLong;
import static org.apache.ignite.internal.processors.cluster.baseline.autoadjust.BaselineTopologyUpdater.DFLT_BASELINE_AUTO_ADJUST_LOG_INTERVAL;

/**
 * This class able to add task of set baseline with timeout to queue. In one time only one task can be in queue. Every
 * next queuing task evicted previous one.
 */
class BaselineAutoAdjustScheduler {
    /** Timeout processor. */
    private final GridTimeoutProcessor timeoutProcessor;

    /** Executor of set baseline operation. */
    private final BaselineAutoAdjustExecutor baselineAutoAdjustExecutor;

    /** Last scheduled task for adjust new baseline. It needed for removing from queue. */
    private BaselineMultiplyUseTimeoutObject baselineTimeoutObj;

    /** */
    private final IgniteLogger log;

    /**
     * @param timeoutProcessor Timeout processor.
     * @param baselineAutoAdjustExecutor Executor of set baseline operation.
     * @param log Log object.
     */
    public BaselineAutoAdjustScheduler(GridTimeoutProcessor timeoutProcessor,
        BaselineAutoAdjustExecutor baselineAutoAdjustExecutor, IgniteLogger log) {
        this.timeoutProcessor = timeoutProcessor;
        this.baselineAutoAdjustExecutor = baselineAutoAdjustExecutor;
        this.log = log;
    }

    /**
     * Adds a new task to queue based on the given {@code baselineAutoAdjustData} with delay and remove previous one.
     * A new task can be rejected in case of the given {@code baselineAutoAdjustData} is expired or
     * the target topology version is less than the already scheduled version.
     *
     * @param baselineAutoAdjustData Data for changing baseline.
     * @param delay Delay after which set baseline should be started.
     * @return {@code true} If a new task was successfully scheduled.
     */
    public synchronized boolean schedule(BaselineAutoAdjustData baselineAutoAdjustData, long delay) {
        if (baselineAutoAdjustExecutor.isExecutionExpired(baselineAutoAdjustData)) {
            if (log.isDebugEnabled())
                log.debug("Baseline auto adjust data is expired (will not be scheduled) [data=" + baselineAutoAdjustData + ']');

            return false;
        }

        if (baselineTimeoutObj != null) {
            long targetVer = baselineAutoAdjustData.getTargetTopologyVersion();
            long alreadyScheduledVer = baselineTimeoutObj.baselineAutoAdjustData.getTargetTopologyVersion();

            if (alreadyScheduledVer > targetVer) {
                if (log.isDebugEnabled()) {
                    log.debug("Baseline auto adjust data is targeted to obsolete version (will not be scheduled) " +
                        "[data=" + baselineAutoAdjustData + ", scheduled=" + baselineTimeoutObj.baselineAutoAdjustData + ']');
                }

                return false;
            }

            timeoutProcessor.removeTimeoutObject(baselineTimeoutObj);
        }

        boolean added = timeoutProcessor.addTimeoutObject(
            baselineTimeoutObj = new BaselineMultiplyUseTimeoutObject(
                baselineAutoAdjustData,
                delay, baselineAutoAdjustExecutor,
                timeoutProcessor,
                log
            )
        );

        if (log.isDebugEnabled()) {
            log.info("New baseline timeout object was " + (added ? "successfully scheduled " : " rejected ") +
                " [data=" + baselineTimeoutObj.baselineAutoAdjustData + ']');
        }

        return added;
    }

    /**
     * @return Time of last scheduled task or -1 if it doesn't exist.
     */
    public synchronized long lastScheduledTaskTime() {
        if (baselineTimeoutObj == null)
            return -1;

        long lastScheduledTaskTime = baselineTimeoutObj.getTotalEndTime() - System.currentTimeMillis();

        return lastScheduledTaskTime < 0 ? -1 : lastScheduledTaskTime;
    }

    /**
     * @param data Baseline data for adjust.
     * @return {@code true} If baseline auto-adjust shouldn't be executed for given data.
     */
    boolean isExecutionExpired(BaselineAutoAdjustData data) {
        return baselineAutoAdjustExecutor.isExecutionExpired(data);
    }

    /**
     * Timeout object of baseline auto-adjust operation. This object able executing several times: some first times for
     * logging of expecting auto-adjust and last time for baseline adjust.
     */
    private static class BaselineMultiplyUseTimeoutObject implements GridTimeoutObject {
        /** Interval between logging of info about next baseline auto-adjust. */
        private static final long AUTO_ADJUST_LOG_INTERVAL =
            getLong(IGNITE_BASELINE_AUTO_ADJUST_LOG_INTERVAL, DFLT_BASELINE_AUTO_ADJUST_LOG_INTERVAL);

        /** Last data for set new baseline. */
        private final BaselineAutoAdjustData baselineAutoAdjustData;

        /** Executor of set baseline operation. */
        private final BaselineAutoAdjustExecutor baselineAutoAdjustExecutor;

        /** Timeout processor. */
        private final GridTimeoutProcessor timeoutProcessor;

        /** */
        private final IgniteLogger log;

        /** End time of whole life of this object. It represent time when auto-adjust will be executed. */
        private final long totalEndTime;

        /** Timeout ID. */
        private final IgniteUuid id = IgniteUuid.randomUuid();

        /** End time of one iteration of this timeout object. */
        private long endTime;

        /**
         * @param data Data for changing baseline.
         * @param executionTimeout Delay after which set baseline should be started.
         * @param executor Executor of set baseline operation.
         * @param processor Timeout processor.
         * @param log Log object.
         */
        protected BaselineMultiplyUseTimeoutObject(
            BaselineAutoAdjustData data,
            long executionTimeout,
            BaselineAutoAdjustExecutor executor,
            GridTimeoutProcessor processor,
            IgniteLogger log
        ) {
            baselineAutoAdjustData = data;
            baselineAutoAdjustExecutor = executor;
            timeoutProcessor = processor;
            this.log = log;
            endTime = calculateEndTime(executionTimeout);
            this.totalEndTime = U.currentTimeMillis() + executionTimeout;
        }

        /**
         * @param timeout Remaining time to baseline adjust.
         * @return Calculated end time to next iteration.
         */
        private long calculateEndTime(long timeout) {
            return U.currentTimeMillis() + (AUTO_ADJUST_LOG_INTERVAL < timeout ? AUTO_ADJUST_LOG_INTERVAL : timeout);
        }

        /** {@inheritDoc}. */
        @Override public IgniteUuid timeoutId() {
            return id;
        }

        /** {@inheritDoc}. */
        @Override public long endTime() {
            return endTime;
        }

        /** {@inheritDoc}. */
        @Override public void onTimeout() {
            if (baselineAutoAdjustExecutor.isExecutionExpired(baselineAutoAdjustData))
                return;

            long lastScheduledTaskTime = totalEndTime - System.currentTimeMillis();

            if (lastScheduledTaskTime <= 0) {
                if (log.isInfoEnabled())
                    log.info("Baseline auto-adjust will be executed right now.");

                baselineAutoAdjustExecutor.execute(baselineAutoAdjustData);
            }
            else {
                if (log.isInfoEnabled())
                    log.info("Baseline auto-adjust will be executed in '" + lastScheduledTaskTime + "' ms.");

                endTime = calculateEndTime(lastScheduledTaskTime);

                timeoutProcessor.addTimeoutObject(this);
            }
        }

        /**
         * @return End time of whole life of this object. It represent time when auto-adjust will be executed.
         */
        public long getTotalEndTime() {
            return totalEndTime;
        }
    }
}
