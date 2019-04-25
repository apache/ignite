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

package org.apache.ignite.platform.plugin.cache;

import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.plugin.CachePluginConfiguration;
import org.apache.ignite.plugin.platform.PlatformCachePluginConfigurationClosure;

import java.util.ArrayList;
import java.util.Collections;

/**
 * Test config closure.
 */
public class PlatformTestCachePluginConfigurationClosure implements PlatformCachePluginConfigurationClosure {
    /** {@inheritDoc} */
    @Override public void apply(CacheConfiguration cacheConfiguration, BinaryRawReader reader) {
        ArrayList<CachePluginConfiguration> cfgs = new ArrayList<>();

        if (cacheConfiguration.getPluginConfigurations() != null) {
            Collections.addAll(cfgs, cacheConfiguration.getPluginConfigurations());
        }

        PlatformTestCachePluginConfiguration plugCfg = new PlatformTestCachePluginConfiguration();

        plugCfg.setPluginProperty(reader.readString());

        cfgs.add(plugCfg);

        cacheConfiguration.setPluginConfigurations(cfgs.toArray(new CachePluginConfiguration[cfgs.size()]));
    }
}
