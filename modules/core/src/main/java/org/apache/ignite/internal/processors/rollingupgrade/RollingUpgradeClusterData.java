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

package org.apache.ignite.internal.processors.rollingupgrade;

import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.rollingupgrade.feature.IgniteComponentFeatureSet;
import org.apache.ignite.internal.processors.rollingupgrade.feature.IgniteNodeFeatureSet;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/** */
public class RollingUpgradeClusterData implements Message {
    /** */
    @Order(0)
    boolean isVersionUpgradeEnabled;

    /** */
    @Order(1)
    UUID curFinalizeProcId;

    /** */
    @Order(2)
    IgniteComponentFeatureSet[] activeFeatures;

    /** */
    @Order(3)
    @Nullable IgniteComponentFeatureSet[] prevActiveFeatures;

    /** */
    public RollingUpgradeClusterData() {
        // No-op.
    }

    /** */
    public RollingUpgradeClusterData(
        boolean isVersionUpgradeEnabled,
        UUID curFinalizeProcId,
        IgniteNodeFeatureSet activeFeatures,
        @Nullable IgniteNodeFeatureSet prevActiveFeatures
    ) {
        this.isVersionUpgradeEnabled = isVersionUpgradeEnabled;
        this.curFinalizeProcId = curFinalizeProcId;
        this.activeFeatures = activeFeatures.values();
        this.prevActiveFeatures = prevActiveFeatures == null ? null : prevActiveFeatures.values();
    }

    /** */
    public IgniteNodeFeatureSet activeFeatures() {
        return new IgniteNodeFeatureSet(activeFeatures);
    }

    /** */
    @Nullable public IgniteNodeFeatureSet previousActiveFeatures() {
        return prevActiveFeatures == null ? null : new IgniteNodeFeatureSet(prevActiveFeatures);
    }
}
