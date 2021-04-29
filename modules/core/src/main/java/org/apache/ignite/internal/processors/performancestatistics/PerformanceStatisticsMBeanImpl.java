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
import org.apache.ignite.mxbean.PerformanceStatisticsMBean;

/**
 * {@link PerformanceStatisticsMBean} implementation.
 */
public class PerformanceStatisticsMBeanImpl implements PerformanceStatisticsMBean {
    /** Kernal context. */
    private final GridKernalContext ctx;

    /** @param ctx Kernal context. */
    public PerformanceStatisticsMBeanImpl(GridKernalContext ctx) {
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        ctx.performanceStatistics().startCollectStatistics();
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteCheckedException {
        ctx.performanceStatistics().stopCollectStatistics();
    }

    /** {@inheritDoc} */
    @Override public void rotate() throws IgniteCheckedException {
        ctx.performanceStatistics().rotateCollectStatistics();
    }

    /** {@inheritDoc} */
    @Override public boolean started() {
        return ctx.performanceStatistics().enabled();
    }
}
