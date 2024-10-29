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
 * I/O operations tracker that does nothing.
 */
public class NoOpIoTracker implements IoTracker {
    /** */
    public static final IoTracker INSTANCE = new NoOpIoTracker();

    /** {@inheritDoc} */
    @Override public boolean startTracking() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void stopTracking() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Nullable @Override public AtomicLong processedRowsCounter(String action) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void flush() {
        // No-op.
    }
}
