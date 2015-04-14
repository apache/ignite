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

import java.io.*;
import java.util.*;

/**
 * Run ignite node. 
 */
public class IgniteNodeRunner {
    /** VM ip finder for TCP discovery. */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryMulticastIpFinder(){{
        setAddresses(Collections.singleton("127.0.0.1:47500..47509"));
    }};

    public static final char DELIM = ' ';
    
    public static final String CACHE_CONFIGURATION_TMP_FILE = "/tmp/cacheConfiguration.tmp";

    public static void main(String[] args) throws Exception {
        try {
            X.println(GridJavaProcess.PID_MSG_PREFIX + U.jvmPid());

            X.println("Starting Ignite Node... Args" + Arrays.toString(args));

            IgniteConfiguration cfg = configuration(args);

            cfg.setCacheConfiguration(cacheConfiguration());

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

    public static IgniteConfiguration configuration(String[] args) throws Exception {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setLocalHost("127.0.0.1");

        cfg.setNodeId((args != null && args.length >= 1) ? UUID.fromString(args[0]) : null);
        //-------
        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        cfg.setMarshaller(new OptimizedMarshaller(false));

        return cfg;
    }

    public static void storeToFile(CacheConfiguration cc) throws IOException {
        // TODO use actual config.
        cc = new CacheConfiguration();
        
        File ccfgTmpFile = new File(CACHE_CONFIGURATION_TMP_FILE);

        ccfgTmpFile.createNewFile();
//        if (!ccfgTmpFile.createNewFile())
//            throw new IgniteSpiException("File was not created.");
        
        try(ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(ccfgTmpFile))) {
            out.writeObject(cc);
        }
    }

    public static CacheConfiguration cacheConfiguration() throws Exception {
        File ccfgTmpFile = new File(CACHE_CONFIGURATION_TMP_FILE);
        
        try(ObjectInputStream in = new ObjectInputStream(new FileInputStream(ccfgTmpFile))) {
            return (CacheConfiguration)in.readObject();
        }
    }
}
