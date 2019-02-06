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

package org.apache.ignite.internal.cluster;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedBooleanProperty;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedLongProperty;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;
import org.apache.ignite.internal.util.future.GridFutureAdapter;

import static org.apache.ignite.internal.processors.configuration.distributed.DistributedBooleanProperty.detachedProperty;
import static org.apache.ignite.internal.processors.configuration.distributed.DistributedLongProperty.detachedProperty;

/**
 * Distributed baseline configuration.
 */
public class DistributedBaselineConfiguration {
    /** Value of manual baseline control or auto adjusting baseline. */
    private DistributedBooleanProperty baselineAutoAdjustEnabled;

    /**
     * Value of time which we would wait before the actual topology change since last discovery event(node join/exit).
     */
    private DistributedLongProperty baselineAutoAdjustTimeout;

    /**
     * @param cfg Static config.
     * @param isp Subscription processor.
     */
    public DistributedBaselineConfiguration(IgniteConfiguration cfg, GridInternalSubscriptionProcessor isp) {
        baselineAutoAdjustEnabled = detachedProperty("baselineAutoAdjustEnabled", cfg.isInitBaselineAutoAdjustEnabled());
        baselineAutoAdjustTimeout = detachedProperty("baselineAutoAdjustTimeout", cfg.getInitBaselineAutoAdjustTimeout());

        isp.registerDistributedConfigurationListener(
            dispatcher -> {
                dispatcher.registerProperty(baselineAutoAdjustEnabled);
                dispatcher.registerProperty(baselineAutoAdjustTimeout);
            }
        );
    }

    /**
     * @return Value of manual baseline control or auto adjusting baseline.
     */
    public boolean isBaselineAutoAdjustEnabled() {
        return baselineAutoAdjustEnabled.value();
    }

    /**
     * @param baselineAutoAdjustEnabled Value of manual baseline control or auto adjusting baseline.
     * @throws IgniteCheckedException if failed.
     */
    public GridFutureAdapter<?> updateBaselineAutoAdjustEnabledAsync(boolean baselineAutoAdjustEnabled)
        throws IgniteCheckedException {
        return this.baselineAutoAdjustEnabled.propagateAsync(baselineAutoAdjustEnabled);
    }

    /**
     * @return Value of time which we would wait before the actual topology change since last discovery event(node
     * join/exit).
     */
    public long getBaselineAutoAdjustTimeout() {
        return baselineAutoAdjustTimeout.value();
    }

    /**
     * @param baselineAutoAdjustTimeout Value of time which we would wait before the actual topology change since last
     * discovery event(node join/exit).
     * @throws IgniteCheckedException If failed.
     */
    public GridFutureAdapter<?> updateBaselineAutoAdjustTimeoutAsync(
        long baselineAutoAdjustTimeout) throws IgniteCheckedException {
        return this.baselineAutoAdjustTimeout.propagateAsync(baselineAutoAdjustTimeout);
    }
}
