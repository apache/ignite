/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cluster;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.cluster.DistributedBaselineConfiguration;
import org.apache.ignite.mxbean.BaselineAutoAdjustMXBean;

/**
 * {@link BaselineAutoAdjustMXBean} implementation.
 */
public class BaselineAutoAdjustMXBeanImpl implements BaselineAutoAdjustMXBean {
    /** */
    private final DistributedBaselineConfiguration baselineConfiguration;

    private final GridClusterStateProcessor state;

    /**
     * @param ctx Context.
     */
    public BaselineAutoAdjustMXBeanImpl(GridKernalContext ctx) {
        baselineConfiguration = ctx.state().baselineConfiguration();
        state = ctx.state();
    }

    /** {@inheritDoc} */
    @Override public boolean isAutoAdjustmentEnabled() {
        return baselineConfiguration.isBaselineAutoAdjustEnabled();
    }

    /** {@inheritDoc} */
    @Override public long getAutoAdjustmentTimeout() {
        return baselineConfiguration.getBaselineAutoAdjustTimeout();
    }

    /** {@inheritDoc} */
    @Override public long getTimeUntilAutoAdjust() {
        return state.baselineAutoAdjustStatus().getTimeUntilAutoAdjust();
    }

    /** {@inheritDoc} */
    @Override public String getTaskState() {
        return state.baselineAutoAdjustStatus().getTaskState().toString();
    }

    /** {@inheritDoc} */
    @Override public void setAutoAdjustmentEnabled(boolean enabled) {
        try {
            baselineConfiguration.updateBaselineAutoAdjustEnabledAsync(enabled).get();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void setAutoAdjustmentTimeout(long timeout) {
        try {
            baselineConfiguration.updateBaselineAutoAdjustTimeoutAsync(timeout).get();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }
}
