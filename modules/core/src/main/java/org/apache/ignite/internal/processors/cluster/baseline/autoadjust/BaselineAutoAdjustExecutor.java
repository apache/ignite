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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.cluster.IgniteClusterImpl;

/**
 * This executor try to set new baseline by given data.
 */
class BaselineAutoAdjustExecutor {
    /** */
    private final IgniteLogger log;

    /** */
    private final IgniteClusterImpl cluster;

    /** Service for execute this task in async. */
    private final ExecutorService executorService;

    /** {@code true} if baseline auto-adjust enabled. */
    private final BooleanSupplier isBaselineAutoAdjustEnabled;

    /** This protect from execution more than one task at same moment. */
    private final Lock executionGuard = new ReentrantLock();

    /**
     * @param log Logger.
     * @param cluster Ignite cluster.
     * @param executorService Thread pool for changing baseline.
     * @param enabledSupplier Supplier return {@code true} if baseline auto-adjust enabled.
     */
    public BaselineAutoAdjustExecutor(IgniteLogger log, IgniteClusterImpl cluster, ExecutorService executorService,
        BooleanSupplier enabledSupplier) {
        this.log = log;
        this.cluster = cluster;
        this.executorService = executorService;
        isBaselineAutoAdjustEnabled = enabledSupplier;
    }

    /**
     * Try to set baseline if all conditions it allowed.
     *
     * @param data Data for operation.
     */
    public void execute(BaselineAutoAdjustData data) {
        executorService.submit(() ->
            {
                if (isExecutionExpired(data))
                    return;

                executionGuard.lock();
                try {
                    if (isExecutionExpired(data))
                        return;

                    cluster.triggerBaselineAutoAdjust(data.getTargetTopologyVersion());
                }
                catch (IgniteException e) {
                    log.error("Error during baseline changing", e);
                }
                finally {
                    data.onAdjust();

                    executionGuard.unlock();
                }
            }
        );
    }

    /**
     * @param data Baseline data for adjust.
     * @return {@code true} If baseline auto-adjust shouldn't be executed for given data.
     */
    public boolean isExecutionExpired(BaselineAutoAdjustData data) {
        return data.isInvalidated() || !isBaselineAutoAdjustEnabled.getAsBoolean();
    }
}
