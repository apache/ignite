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

/**
 * Logic used to protect memory (mainly, Checkpoint Buffer) from exhaustion using exponential backoff.
 */
class ExponentialBackoffThrottlingStrategy implements ThrottlingStrategy {
    /**
     * Starting throttle time. Limits write speed to 1000 MB/s.
     */
    private static final long STARTING_THROTTLE_NANOS = 4000;

    /**
     * Backoff ratio. Each next park will be this times longer.
     */
    private static final double BACKOFF_RATIO = 1.05;

    /**
     * Exponential backoff used to throttle threads.
     */
    private final ExponentialBackoff backoff = new ExponentialBackoff(STARTING_THROTTLE_NANOS, BACKOFF_RATIO);

    /** {@inheritDoc} */
    @Override public long protectionParkTime() {
        return backoff.nextDuration();
    }

    /** {@inheritDoc} */
    @Override public boolean reset() {
        return backoff.reset();
    }
}
