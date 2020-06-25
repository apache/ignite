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

package org.apache.ignite.internal.processors.performancestatistics;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;

/**
 * {@link IgnitePerformanceStatisticsMBean} implementation.
 */
public class IgnitePerformanceStatisticsMbeanImpl implements IgnitePerformanceStatisticsMBean {
    /** Kernal context. */
    private final GridKernalContext ctx;

    /** @param ctx Kernal context. */
    public IgnitePerformanceStatisticsMbeanImpl(GridKernalContext ctx) {
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        ctx.performanceStatistics().startStatistics().get();
    }
    /** {@inheritDoc} */
    @Override public void stop() throws IgniteCheckedException {
        ctx.performanceStatistics().stopStatistics().get();
    }

    /** {@inheritDoc} */
    @Override public boolean enabled() {
        return ctx.performanceStatistics().statisticsEnabled();
    }
}
