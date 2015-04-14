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

package org.apache.ignite.internal.processors.cache.multijvm;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.*;

import java.util.*;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.*;
import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;

/**
 * Run ignite node. 
 */
public class IgniteNodeRunner {
    /** VM ip finder for TCP discovery. */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryMulticastIpFinder(){{
        setAddresses(Collections.singleton("127.0.0.1:47500..47509"));
    }};

    public static final char DELIM = ' ';

    public static void main(String[] args) throws Exception {
        try {
            X.println(GridJavaProcess.PID_MSG_PREFIX + U.jvmPid());

            X.println("Starting Ignite Node... Args" + Arrays.toString(args));

            IgniteConfiguration cfg = configuration(args);

//            cfg.setCacheConfiguration(cacheConfiguration());

            Ignite ignite = Ignition.start(cfg);
        }
        catch (Throwable e) {
            e.printStackTrace();
            
            System.exit(1);
        }
    }

    public static String asParams(UUID id, IgniteConfiguration cfg) {
        return id.toString() + DELIM + cfg.getGridName();
    }

    public static IgniteConfiguration configuration(String[] args) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        // TODO uncomment
//        cfg.setNodeId(UUID.fromString(args[0]));
        //-------
        TcpDiscoverySpi disco = new TcpDiscoverySpi();

//        disco.setMaxMissedHeartbeats(Integer.MAX_VALUE);

        disco.setIpFinder(ipFinder);

//        if (isDebug())
//            disco.setAckTimeout(Integer.MAX_VALUE);

        cfg.setDiscoverySpi(disco);

        cfg.setMarshaller(new OptimizedMarshaller(false));

//        if (offHeapValues())
//            cfg.setSwapSpaceSpi(new GridTestSwapSpaceSpi());

//        cfg.getTransactionConfiguration().setTxSerializableEnabled(true);

        return cfg;
    }

    // TODO implement.
    private static CacheConfiguration cacheConfiguration() {
        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setStartSize(1024);
        cfg.setAtomicWriteOrderMode(PRIMARY);
        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setNearConfiguration(new NearCacheConfiguration());
        cfg.setWriteSynchronizationMode(FULL_SYNC);
        cfg.setEvictionPolicy(null);

//        if (offHeapValues()) {
//            ccfg.setMemoryMode(CacheMemoryMode.OFFHEAP_VALUES);
//            ccfg.setOffHeapMaxMemory(0);
//        }

        cfg.setEvictSynchronized(false);

//        cfg.setAtomicityMode(atomicityMode());
        cfg.setSwapEnabled(true);

        cfg.setRebalanceMode(CacheRebalanceMode.SYNC);

        return cfg;
    }
}
