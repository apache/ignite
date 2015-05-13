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

package org.apache.ignite.internal.processors.cache.multijvm.framework;

import com.thoughtworks.xstream.*;
import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.*;

import java.io.*;
import java.util.*;

/**
 * Run ignite node.
 */
public class IgniteNodeRunner {
    /** */
    private static final String IGNITE_CONFIGURATION_FILE = System.getProperty("java.io.tmpdir") +
        File.separator + "igniteConfiguration.tmp";

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

            IgniteConfiguration cfg = readFromFile();

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

    public static void storeToFile(IgniteConfiguration cfg) throws IOException {
        try(OutputStream out = new BufferedOutputStream(new FileOutputStream(IGNITE_CONFIGURATION_FILE))) {
            cfg.setMBeanServer(null);
            cfg.setMarshaller(null);
            cfg.setDiscoverySpi(null);

            new XStream().toXML(cfg, out);
        }
    }

    private static IgniteConfiguration readFromFile() throws FileNotFoundException {
        BufferedReader cfgReader = new BufferedReader(new FileReader(IGNITE_CONFIGURATION_FILE));
        
        IgniteConfiguration cfg = (IgniteConfiguration)new XStream().fromXML(cfgReader);
        
        cfg.setMarshaller(new OptimizedMarshaller(false));

        TcpDiscoverySpi disco = new TcpDiscoverySpi();
        disco.setIpFinder(new TcpDiscoveryMulticastIpFinder());
        
        cfg.setDiscoverySpi(disco);
        
        return cfg;
    }
}
