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
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.*;
import org.apache.ignite.testframework.junits.*;

import java.io.*;
import java.util.*;

/**
 * Run ignite node. 
 */
public class IgniteNodeRunner {
    /** VM ip finder for TCP discovery. */
    public static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryMulticastIpFinder();
    
    /** */
    private static final String CACHE_CONFIGURATION_TMP_FILE = "/tmp/cacheConfiguration.tmp";

    /**
     * Starts {@link Ignite} instance accorging to given arguments.
     *  
     * @param args Arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        try {
            X.println(GridJavaProcess.PID_MSG_PREFIX + U.jvmPid());

            X.println("Starting Ignite Node... Args" + Arrays.toString(args));

            IgniteConfiguration cfg = configuration(args);

            Ignition.start(cfg);
        }
        catch (Throwable e) {
            e.printStackTrace();
            
            System.exit(1);
        }
    }

    /** 
     * @param id Grid id.
     * @param cfg Configuration.
     * @return Given paramethers as command line string arguments.
     */
    public static String asParams(UUID id, IgniteConfiguration cfg) {
        return id.toString() + ' ' + cfg.getGridName();
    }

    /** 
     * @param args Command line args.
     * @return Ignite configuration.
     * @throws Exception If failed.
     */
    private static IgniteConfiguration configuration(String[] args) throws Exception {
        // Parse args.
        assert args != null && args.length >= 1;

        final UUID nodeId = UUID.fromString(args[0]);
        final String gridName = args[1];
        
        // Configuration.
        IgniteConfiguration cfg = GridAbstractTest.getConfiguration0(gridName, new IgniteTestResources(),
            GridCachePartitionedMultiJvmFullApiSelfTest.class, false);
        
        TcpDiscoverySpi disco = new TcpDiscoverySpi();

//        disco.setMaxMissedHeartbeats(Integer.MAX_VALUE);

        disco.setIpFinder(ipFinder);

//        if (isDebug())
//            disco.setAckTimeout(Integer.MAX_VALUE);

        cfg.setDiscoverySpi(disco);

//        cfg.setCacheConfiguration(cacheConfiguration());

        cfg.setMarshaller(new OptimizedMarshaller(false));
////        ----------------
////        if (offHeapValues())
////            cfg.setSwapSpaceSpi(new GridTestSwapSpaceSpi());
////        ----------------
//        cfg.getTransactionConfiguration().setTxSerializableEnabled(true);
//
////        ---------------
//        Special.
        cfg.setLocalHost("127.0.0.1");

        cfg.setIncludeProperties();

        cfg.setNodeId(nodeId);

        return cfg;
    }

    /**
     * Stors given cache configuration to the file.
     *
     * @param cc Cache configuration.
     * @throws IOException If exception.
     */
    public static void storeToFile(CacheConfiguration cc) throws IOException {
        File ccfgTmpFile = new File(CACHE_CONFIGURATION_TMP_FILE);

        boolean created = ccfgTmpFile.createNewFile();

        try(ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(ccfgTmpFile))) {
            out.writeObject(cc);
        }
    }

    /**
     * Reads cache configuration from the file.
     *  
     * @return Cache configuration.
     * @throws Exception If exception.
     */
    private static CacheConfiguration cacheConfiguration() throws Exception {
        File ccfgTmpFile = new File(CACHE_CONFIGURATION_TMP_FILE);

        try(ObjectInputStream in = new ObjectInputStream(new FileInputStream(ccfgTmpFile))) {
            return (CacheConfiguration)in.readObject();
        }
    }
}
