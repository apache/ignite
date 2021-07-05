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
package org.apache.ignite.snippets;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.PageReplacementMode;

public class ReplacementPolicies {
    public static void main(String[] args) {
        segmentedLRU();
    }

    public static void segmentedLRU() {
        //tag::segmentedLRU[]
        // Node configuration.
        IgniteConfiguration cfg = new IgniteConfiguration();

        // Memory configuration.
        DataStorageConfiguration storageCfg = new DataStorageConfiguration();

        // Creating a new data region.
        DataRegionConfiguration regionCfg = new DataRegionConfiguration();

        // Region name.
        regionCfg.setName("persistent_data_region");

        // Enabling persistence.
        regionCfg.setPersistenceEnabled(true);

        // 20 GB max size (RAM).
        regionCfg.setMaxSize(20L * 1024 * 1024 * 1024);

        // Enabling SEGMENTED_LRU page replacement for this region.
        regionCfg.setPageReplacementMode(PageReplacementMode.SEGMENTED_LRU);

        // Setting the data region configuration.
        storageCfg.setDataRegionConfigurations(regionCfg);

        // Applying the new configuration.
        cfg.setDataStorageConfiguration(storageCfg);
        //end::segmentedLRU[]

        try (Ignite ignite = Ignition.start(new IgniteConfiguration().setDataStorageConfiguration(storageCfg))) {

        }
    }
}
