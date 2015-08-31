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

package org.apache.ignite.events;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Generator for local atomically incremented IDs for grid events.
 */
final class EventLocalOrder {
    /** Generator implementation. */
    private static final AtomicLong gen = new AtomicLong(0);

    /**
     * No-arg constructor enforces the singleton.
     */
    private EventLocalOrder() {
        // No-op.
    }

    /**
     * Gets next atomically incremented local event order.
     *
     * @return Next atomically incremented local event order.
     */
    public static long nextOrder() {
        return gen.getAndIncrement();
    }
}