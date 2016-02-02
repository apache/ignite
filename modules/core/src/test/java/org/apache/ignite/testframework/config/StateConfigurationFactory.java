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

package org.apache.ignite.testframework.config;

import java.util.Arrays;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteClosure;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class StateConfigurationFactory implements ConfigurationFactory {
    /** */
    private final IgniteClosure<IgniteConfiguration, Void>[][] igniteParams;

    /** */
    private final int[] igniteCfgState;

    /** */
    private final IgniteClosure<CacheConfiguration, Void>[][] cacheParams;

    /** */
    private final int[] cacheCfgState;

    /**
     * @param igniteParams Ignite Params.
     * @param igniteCfgState Ignite configuration state.
     * @param cacheParams Cache Params.
     * @param cacheCfgState Cache configuration state.
     */
    public StateConfigurationFactory(IgniteClosure<IgniteConfiguration, Void>[][] igniteParams,
        @Nullable int[] igniteCfgState,
        IgniteClosure<CacheConfiguration, Void>[][] cacheParams,
        @Nullable int[] cacheCfgState) {
        this.igniteParams = igniteParams;
        this.igniteCfgState = igniteCfgState;
        this.cacheParams = cacheParams;
        this.cacheCfgState = cacheCfgState;
    }

    /** {@inheritDoc} */
    @Override public IgniteConfiguration getConfiguration(String gridName) {
        System.out.println("[StateConfigurationFactory] Getting IgniteConfiguration with next state: " 
            + Arrays.toString(igniteCfgState));

        IgniteConfiguration cfg = new IgniteConfiguration();

        for (int i = 0; i < igniteCfgState.length; i++) {
            int var = igniteCfgState[i];

            IgniteClosure<IgniteConfiguration, Void> cfgC = igniteParams[i][var];

            if (cfgC != null)
                cfgC.apply(cfg);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override public CacheConfiguration cacheConfiguration(String gridName) {
        System.out.println("[StateConfigurationFactory] Getting CacheConfiguration with next state: " 
            + Arrays.toString(cacheCfgState));
        
        CacheConfiguration cfg = new CacheConfiguration();

        for (int i = 0; i < cacheCfgState.length; i++) {
            int var = cacheCfgState[i];

            IgniteClosure<CacheConfiguration, Void> cfgC = cacheParams[i][var];

            if (cfgC != null)
                cfgC.apply(cfg);
        }

        return cfg;
    }
}
