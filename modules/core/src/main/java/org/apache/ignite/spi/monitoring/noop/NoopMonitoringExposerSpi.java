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

package org.apache.ignite.spi.monitoring.noop;

import org.apache.ignite.internal.processors.monitoring.GridMonitoringManager;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiNoop;
import org.apache.ignite.spi.monitoring.MonitoringExposerSpi;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
@IgniteSpiNoop
public class NoopMonitoringExposerSpi extends IgniteSpiAdapter implements MonitoringExposerSpi {
    @Override public void setMonitoringProcessor(GridMonitoringManager gridMonitoringManager) {
        // No-op.
    }

    @Override public void spiStart(@Nullable String igniteInstanceName) throws IgniteSpiException {
        // No-op.
    }

    @Override public void spiStop() throws IgniteSpiException {
        // No-op.
    }
}
