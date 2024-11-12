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

package org.apache.ignite.internal.processors.query.calcite.exec.tracker;

import java.util.concurrent.atomic.AtomicLong;
import org.jetbrains.annotations.Nullable;

/**
 * I/O operations tracker interface.
 */
public interface IoTracker {
    /**
     * Start tracking of I/O operations performed by current thread.
     *
     * @return {@code True} if tracking is started and wasn't started before.
     */
    public boolean startTracking();

    /** Stop tracking and save result. */
    public void stopTracking();

    /**
     * Register counter for processed rows.
     *
     * @param action Action with rows.
     */
    @Nullable public AtomicLong processedRowsCounter(String action);

    /**
     * Flush tracked data.
     */
    public void flush();
}
