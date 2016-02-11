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
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.config.generator.ConfigurationParameter;

/**
 * TODO: delete the class.
 */
public class FullApiStateConfigurationFactory extends StateConfigurationFactory {
    /**
     * @param igniteParams Ignite params.
     * @param igniteCfgState Ignite config state.
     * @param cacheParams Cache params.
     * @param cacheCfgState Cache config state.
     */
    public FullApiStateConfigurationFactory(
        ConfigurationParameter<IgniteConfiguration>[][] igniteParams, int[] igniteCfgState,
        ConfigurationParameter<CacheConfiguration>[][] cacheParams, int[] cacheCfgState) {
        super(igniteParams, igniteCfgState, cacheParams, cacheCfgState);
    }

    /**
     * @param cacheParams Params.
     * @param cacheCfgState State.
     */
    public FullApiStateConfigurationFactory(
        ConfigurationParameter<CacheConfiguration>[][] cacheParams, int[] cacheCfgState) {
        super(cacheParams, cacheCfgState);
    }

    /** {@inheritDoc} */
    @Override public IgniteConfiguration getConfiguration(String gridName, IgniteConfiguration srcCfg) {
        IgniteConfiguration cfg = super.getConfiguration(gridName, srcCfg);

//            // Cache abstract.
        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setMaxMissedHeartbeats(Integer.MAX_VALUE);

        disco.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(disco);

        // TODO rewrite.
        cfg.setDiscoverySpi(srcCfg.getDiscoverySpi());

        // Full API
        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setForceServerMode(true);

        // Local cache.
        cfg.getTransactionConfiguration().setTxSerializableEnabled(true);

        return cfg;
    }
}
