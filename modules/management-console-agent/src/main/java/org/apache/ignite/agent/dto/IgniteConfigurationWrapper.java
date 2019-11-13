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

package org.apache.ignite.agent.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.ignite.configuration.IgniteConfiguration;

/**
 * Cluster configuration.
 */
@JsonIgnoreProperties(
    ignoreUnknown = true,
    value = {
        "marshaller",
        "mbeanServer",
        "cacheConfiguration",
        "dataStorageConfiguration",
        "gridLogger",
        "loadBalancingSpi",
        "indexingSpi",
        "encryptionSpi",
        "metricExporterSpi",
        "tracingSpi",
        "eventStorageSpi",
        "discoverySpi",
        "communicationSpi",
        "checkpointSpi",
        "failoverSpi",
        "deploymentSpi",
        "collisionSpi",
        "cacheStoreSessionListenerFactories",
        "failureHandler",
        "localEventListeners"
    }
)
public class IgniteConfigurationWrapper extends IgniteConfiguration {
    /**
     * Creates valid grid configuration with all default values.
     */
    public IgniteConfigurationWrapper() {
    }

    /**
     * Creates grid configuration by coping all configuration properties from
     * given configuration.
     *
     * @param cfg Grid configuration to copy from.
     */
    public IgniteConfigurationWrapper(IgniteConfiguration cfg) {
        super(cfg);
    }
}
