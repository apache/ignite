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

package org.apache.ignite.internal.processors.cache.mvcc;

import org.apache.ignite.lang.IgniteReducer;
import org.jetbrains.annotations.Nullable;

/**
 * Vacuum metrics reducer.
 */
public class VacuumMetricsReducer implements IgniteReducer<VacuumMetrics, VacuumMetrics> {
    /** */
    private static final long serialVersionUID = 7063457745963917386L;

    /** */
    private final VacuumMetrics m = new VacuumMetrics();

    /** {@inheritDoc} */
    @Override public boolean collect(@Nullable VacuumMetrics metrics) {
        assert metrics != null;

        m.addCleanupRowsCnt(metrics.cleanupRowsCount());
        m.addScannedRowsCount(metrics.scannedRowsCount());
        m.addSearchNanoTime(metrics.searchNanoTime());
        m.addCleanupNanoTime(metrics.cleanupNanoTime());

        return true;
    }

    /** {@inheritDoc} */
    @Override public VacuumMetrics reduce() {
        return m;
    }
}