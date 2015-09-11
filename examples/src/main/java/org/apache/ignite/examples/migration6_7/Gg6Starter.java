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

package org.apache.ignite.examples.migration6_7;

import java.util.Collections;
import org.gridgain.grid.Grid;
import org.gridgain.grid.GridConfiguration;
import org.gridgain.grid.GridGain;
import org.gridgain.grid.cache.GridCache;
import org.gridgain.grid.cache.GridCacheConfiguration;
import org.gridgain.grid.spi.GridSpiException;
import org.gridgain.grid.spi.discovery.tcp.GridTcpDiscoverySpi;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.GridTcpDiscoveryVmIpFinder;

/**
 */
public class Gg6Starter {
    /** */
    public static final String PARTITIONED = "partitioned";

    public static void main(String[] args) throws Exception {
        try(Grid grid = GridGain.start(configuration())) {
            GridCache<Integer, String> cache = grid.cache(PARTITIONED);

            for (int i = 0; i < 50; i++)
                cache.put(i, "str_" + i);

//            for (int i = 0; i < 50; i++)
//                System.out.println(">>>>> Gg6 client got [i="+i+", val="+cache.get(i)+"]");

            Thread.sleep(Migration6_7.SLEEP);
        }
    }

    private static GridConfiguration configuration() throws GridSpiException {
        GridConfiguration c = new GridConfiguration();

        c.setLocalHost("127.0.0.1");

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();
//        discoSpi.setLocalAddress("127.0.0.1");
        discoSpi.setLocalPort(47500);

        GridTcpDiscoveryVmIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(Collections.singleton("127.0.0.1:47500"));
        discoSpi.setIpFinder(ipFinder);
        c.setDiscoverySpi(discoSpi);

        GridCacheConfiguration cc = new GridCacheConfiguration();
        cc.setName(PARTITIONED);

        c.setCacheConfiguration(cc);

        return c;
    }
}
