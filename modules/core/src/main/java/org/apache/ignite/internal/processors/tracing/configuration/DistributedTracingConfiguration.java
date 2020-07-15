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

package org.apache.ignite.internal.processors.tracing.configuration;

import java.util.HashMap;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedConfigurationProcessor;
import org.apache.ignite.internal.processors.configuration.distributed.SimpleDistributedProperty;
import org.apache.ignite.spi.tracing.TracingConfigurationCoordinates;
import org.apache.ignite.spi.tracing.TracingConfigurationParameters;

/**
 * The wrapper of {@code HashMap<TracingConfigurationCoordinates, TracingConfigurationParameters>}
 * for the distributed metastorage binding.
 */
public class DistributedTracingConfiguration
    extends SimpleDistributedProperty<HashMap<TracingConfigurationCoordinates, TracingConfigurationParameters>> {
    /** */
    private static final String TRACING_CONFIGURATION_DISTRIBUTED_METASTORE_KEY = "tr.config";

    /**
     * Constructor.
     */
    public DistributedTracingConfiguration() {
        super(TRACING_CONFIGURATION_DISTRIBUTED_METASTORE_KEY);
    }

    /**
     * @return A new property that is detached from {@link DistributedConfigurationProcessor}.
     * This means distributed updates are not accessible.
     */
    public static DistributedTracingConfiguration detachedProperty() {
        return new DistributedTracingConfiguration();
    }
}
