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

package org.apache.ignite.internal.util.ipc.shmem;

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.events.EventType.EVT_JOB_MAPPED;
import static org.apache.ignite.events.EventType.EVT_TASK_FAILED;
import static org.apache.ignite.events.EventType.EVT_TASK_FINISHED;

/**
 *
 */
public class IpcSharedMemoryNodeStartup {
    /**
     * @param args Args.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        IgniteConfiguration cfg = new IgniteConfiguration();

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(discoSpi);

        CacheConfiguration metaCacheCfg = new CacheConfiguration();

        metaCacheCfg.setName("partitioned");
        metaCacheCfg.setCacheMode(PARTITIONED);
        metaCacheCfg.setNearConfiguration(null);
        metaCacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        metaCacheCfg.setEvictionPolicy(null);
        metaCacheCfg.setBackups(0);

        CacheConfiguration dataCacheCfg = new CacheConfiguration();

        dataCacheCfg.setName("partitioned");
        dataCacheCfg.setCacheMode(PARTITIONED);
        dataCacheCfg.setNearConfiguration(null);
        dataCacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        dataCacheCfg.setEvictionPolicy(null);
        dataCacheCfg.setBackups(0);

        cfg.setIncludeEventTypes(EVT_TASK_FAILED, EVT_TASK_FINISHED, EVT_JOB_MAPPED);

        try (Ignite ignored = G.start(cfg)) {
            X.println("Press any key to stop grid...");

            System.in.read();
        }
    }
}
