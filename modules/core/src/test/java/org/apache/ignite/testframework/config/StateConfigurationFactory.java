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

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.testframework.config.generator.ConfigurationParameter;

/**
 *
 */
public class StateConfigurationFactory implements ConfigurationFactory {
    /** */
    private final ConfigurationParameter<IgniteConfiguration>[][] igniteParams;

    /** */
    private final int[] igniteCfgState;

    /** */
    private final ConfigurationParameter<CacheConfiguration>[][] cacheParams;

    /** */
    private final int[] cacheCfgState;

    /**
     * @param igniteParams Ignite Params.
     * @param igniteCfgState Ignite configuration state.
     * @param cacheParams Cache Params.
     * @param cacheCfgState Cache configuration state.
     */
    public StateConfigurationFactory(ConfigurationParameter<IgniteConfiguration>[][] igniteParams,
        int[] igniteCfgState,
        ConfigurationParameter<CacheConfiguration>[][] cacheParams,
        int[] cacheCfgState) {
        this.igniteParams = igniteParams;
        this.igniteCfgState = igniteCfgState;
        this.cacheParams = cacheParams;
        this.cacheCfgState = cacheCfgState;
    }

    /**
     * @param cacheCfgState Cache configuration state.
     * @param cacheParams Cache paramethers.
     */
    public StateConfigurationFactory(ConfigurationParameter<CacheConfiguration>[][] cacheParams, int[] cacheCfgState) {
        this(null, null, cacheParams, cacheCfgState);
    }

    /** {@inheritDoc} */
    @Override public IgniteConfiguration getConfiguration(String gridName, IgniteConfiguration srcCfg) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        copyDefaultsFromSource(cfg, srcCfg);

        if (igniteParams == null)
            return cfg;

        for (int i = 0; i < igniteCfgState.length; i++) {
            int var = igniteCfgState[i];

            ConfigurationParameter<IgniteConfiguration> cfgC = igniteParams[i][var];

            if (cfgC != null)
                cfgC.apply(cfg);
        }

        return cfg;
    }

    /**
     * TODO: delete this method and usage.
     *
     * @param cfg Config.
     * @param srcCfg Source config.
     */
    private static void copyDefaultsFromSource(IgniteConfiguration cfg, IgniteConfiguration srcCfg) {
        cfg.setGridName(srcCfg.getGridName());
        cfg.setGridLogger(srcCfg.getGridLogger());
//        cfg.setMarshaller(srcCfg.getMarshaller());
        cfg.setNodeId(srcCfg.getNodeId());
        cfg.setIgniteHome(srcCfg.getIgniteHome());
        cfg.setMBeanServer(srcCfg.getMBeanServer());
//        cfg.setPeerClassLoadingEnabled(srcCfg.isPeerClassLoadingEnabled());
        cfg.setMetricsLogFrequency(srcCfg.getMetricsLogFrequency());
        cfg.setConnectorConfiguration(srcCfg.getConnectorConfiguration());
        cfg.setCommunicationSpi(srcCfg.getCommunicationSpi());
        cfg.setNetworkTimeout(srcCfg.getNetworkTimeout());
        cfg.setDiscoverySpi(srcCfg.getDiscoverySpi());
        cfg.setCheckpointSpi(srcCfg.getCheckpointSpi());
        cfg.setIncludeEventTypes(srcCfg.getIncludeEventTypes());
    }

    /**
     * @return Description.
     */
    public String getIgniteConfigurationDescription(){
        if (igniteParams == null)
            return "";

        SB sb = new SB("[");

        for (int i = 0; i < igniteCfgState.length; i++) {
            int var = igniteCfgState[i];

            ConfigurationParameter<IgniteConfiguration> cfgC = igniteParams[i][var];

            if (cfgC != null) {
                sb.a(cfgC.name());

                if (i + 1 < igniteCfgState.length)
                    sb.a(", ");
            }
        }

        sb.a("]");

        return sb.toString();

    }

    /** {@inheritDoc} */
    @Override public CacheConfiguration cacheConfiguration(String gridName) {
        CacheConfiguration cfg = new CacheConfiguration();

        for (int i = 0; i < cacheCfgState.length; i++) {
            int var = cacheCfgState[i];

            ConfigurationParameter<CacheConfiguration> cfgC = cacheParams[i][var];

            if (cfgC != null)
                cfgC.apply(cfg);
        }

        return cfg;
    }

    /**
     * @return Description.
     */
    public String getCacheConfigurationDescription(){
        if (cacheCfgState == null)
            return "";

        SB sb = new SB("[");

        for (int i = 0; i < cacheCfgState.length; i++) {
            int var = cacheCfgState[i];

            ConfigurationParameter cfgC = cacheParams[i][var];

            if (cfgC != null) {
                sb.a(cfgC.name());

                if (i + 1 < cacheCfgState.length)
                    sb.a(", ");
            }
        }

        sb.a("]");

        return sb.toString();

    }
}
