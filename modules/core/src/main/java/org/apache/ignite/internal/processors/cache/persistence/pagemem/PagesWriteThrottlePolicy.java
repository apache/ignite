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

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import org.apache.ignite.IgniteSystemProperties;

import java.util.concurrent.TimeUnit;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_THROTTLE_LOG_THRESHOLD;

/**
 * Throttling policy, encapsulates logic of delaying write operations.
 */
public interface PagesWriteThrottlePolicy {
    /** Max park time. */
    public long LOGGING_THRESHOLD = TimeUnit.SECONDS.toNanos(IgniteSystemProperties.getInteger
            (IGNITE_THROTTLE_LOG_THRESHOLD, 10));

    /**
     * Callback to apply throttling delay.
     * @param isPageInCheckpoint flag indicating if current page is in scope of current checkpoint.
     */
    void onMarkDirty(boolean isPageInCheckpoint);

    /**
     * Callback to notify throttling policy checkpoint was started.
     */
    void onBeginCheckpoint();

    /**
     * Callback to notify throttling policy checkpoint was finished.
     */
    void onFinishCheckpoint();
}
