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

package org.apache.ignite.platform.plugin;

import java.util.ArrayList;
import java.util.Collections;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.plugin.PluginConfiguration;
import org.apache.ignite.plugin.platform.PlatformPluginConfigurationClosure;
import org.apache.ignite.plugin.platform.PlatformPluginConfigurationClosureFactory;
import org.jetbrains.annotations.NotNull;

/**
 * Test config factory.
 */
public class PlatformTestPluginConfigurationClosureFactory implements PlatformPluginConfigurationClosureFactory,
        PlatformPluginConfigurationClosure {
    /** {@inheritDoc} */
    @Override public int id() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public PlatformPluginConfigurationClosure create() {
        return this;
    }

    /** {@inheritDoc} */
    @Override public void apply(@NotNull IgniteConfiguration cfg, @NotNull BinaryRawReader reader) {
        ArrayList<PluginConfiguration> cfgs = new ArrayList<>();

        if (cfg.getPluginConfigurations() != null) {
            Collections.addAll(cfgs, cfg.getPluginConfigurations());
        }

        PlatformTestPluginConfiguration plugCfg = new PlatformTestPluginConfiguration();

        plugCfg.setPluginProperty(reader.readString());

        cfgs.add(plugCfg);

        cfg.setPluginConfigurations(cfgs.toArray(new PluginConfiguration[cfgs.size()]));
    }
}
