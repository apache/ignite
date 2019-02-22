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

package org.apache.ignite.internal.processors.cluster;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.cluster.DistributedBaselineConfiguration;
import org.apache.ignite.mxbean.BaselineConfigurationXMBean;

/**
 * {@link BaselineConfigurationXMBean} implementation.
 */
public class BaselineConfigurationXMBeanImpl implements BaselineConfigurationXMBean {
    /** */
    private final DistributedBaselineConfiguration baselineConfiguration;

    /**
     * @param ctx Context.
     */
    public BaselineConfigurationXMBeanImpl(GridKernalContext ctx) {
        baselineConfiguration = ctx.cluster().get().baselineConfiguration();
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
    @Override public void enableAutoAdjustment() {
        setAutoAdjustmentEnabled(true);
    }

    /** {@inheritDoc} */
    @Override public void disableAutoAdjustment() {
        setAutoAdjustmentEnabled(false);
    }

    /** {@inheritDoc} */
    @Override public void setAutoAdjustmentEnabled(boolean enabled) {
        try {
            baselineConfiguration.updateBaselineAutoAdjustEnabledAsync(enabled).get();
        }
        catch (IgniteCheckedException e) {
            throw new IllegalStateException(e.getMessage());
        }
    }

    /** {@inheritDoc} */
    @Override public void setAutoAdjustmentTimeout(long timeout) {
        try {
            baselineConfiguration.updateBaselineAutoAdjustTimeoutAsync(timeout).get();
        }
        catch (IgniteCheckedException e) {
            throw new IllegalStateException(e.getMessage());
        }
    }
}
