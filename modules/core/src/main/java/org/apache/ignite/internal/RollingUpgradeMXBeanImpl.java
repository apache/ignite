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

package org.apache.ignite.internal;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.internal.processors.ru.RollingUpgradeProcessor;
import org.apache.ignite.mxbean.RollingUpgradeMXBean;

/**
 *
 */
public class RollingUpgradeMXBeanImpl implements RollingUpgradeMXBean {
    /** Rolling upgrade processor. */
    private final RollingUpgradeProcessor rollingUpgradeProcessor;

    /**
     * @param ctx Context.
     */
    public RollingUpgradeMXBeanImpl(GridKernalContextImpl ctx) {
        rollingUpgradeProcessor = ctx.rollingUpgrade();
    }

    /** {@inheritDoc} */
    @Override public boolean isEnabled() {
        return rollingUpgradeProcessor.getStatus().isEnabled();
    }

    /** {@inheritDoc} */
    @Override public String getInitialVersion() {
        return String.valueOf(rollingUpgradeProcessor.getStatus().getInitialVersion());
    }

    /** {@inheritDoc} */
    @Override public String getUpdateVersion() {
        return String.valueOf(rollingUpgradeProcessor.getStatus().getUpdateVersion());
    }

    /** {@inheritDoc} */
    @Override public List<String> getSupportedFeatures() {
        return rollingUpgradeProcessor.getStatus().getSupportedFeatures().stream()
            .map(IgniteFeatures::name)
            .collect(Collectors.toList());
    }

    /** {@inheritDoc} */
    @Override public void changeMode(boolean enable) {
        rollingUpgradeProcessor.setMode(enable);
    }

}
