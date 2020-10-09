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
package org.apache.ignite.snippets;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

public class RebalancingConfiguration {

    public static void main(String[] args) {
        RebalancingConfiguration rc = new RebalancingConfiguration();

        rc.configure();
    }

    void configure() {
        //tag::ignite-config[]
        IgniteConfiguration cfg = new IgniteConfiguration();
        //tag::pool-size[]

        cfg.setRebalanceThreadPoolSize(4);
        //end::pool-size[]

        CacheConfiguration cacheCfg = new CacheConfiguration("mycache");
        //tag::mode[]

        cacheCfg.setRebalanceMode(CacheRebalanceMode.SYNC);

        //end::mode[]
        //tag::throttling[]

        cfg.setRebalanceBatchSize(2 * 1024 * 1024);
        cfg.setRebalanceThrottle(100);

        //end::throttling[]
        cfg.setCacheConfiguration(cacheCfg);

        // Start a node.
        Ignite ignite = Ignition.start(cfg);
        //end::ignite-config[]

        ignite.close();
    }

}
