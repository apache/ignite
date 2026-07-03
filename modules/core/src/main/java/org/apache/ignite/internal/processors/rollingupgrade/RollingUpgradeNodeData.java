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
import org.apache.ignite.internal.processors.rollingupgrade.feature.IgniteProductFeatures;
import org.apache.ignite.plugin.extensions.communication.Message;

/** */
public class RollingUpgradeNodeData implements Message {
    /** */
    @Order(0)
    boolean isVersionUpgradeEnabled;

    /** */
    @Order(1)
    UUID activeFinalizeProcId;

    /** */
    @Order(2)
    boolean isNodeFenceActive;

    /** */
    @Order(3)
    IgniteProductFeatures activeFeatures;

    /** */
    public RollingUpgradeNodeData() {
        // No-op.
    }

    /** */
    public RollingUpgradeNodeData(
        boolean isVersionUpgradeEnabled,
        UUID activeFinalizeProcId,
        boolean isNodeFenceActive,
        IgniteProductFeatures activeFeatures
    ) {
        this.isVersionUpgradeEnabled = isVersionUpgradeEnabled;
        this.activeFinalizeProcId = activeFinalizeProcId;
        this.isNodeFenceActive = isNodeFenceActive;
        this.activeFeatures = activeFeatures;
    }
}
