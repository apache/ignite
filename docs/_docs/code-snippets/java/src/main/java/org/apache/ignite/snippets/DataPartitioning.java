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

import org.apache.ignite.Ignition;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.junit.jupiter.api.Test;

public class DataPartitioning {

    @Test
     void configurationExample() {
        // tag::cfg[]
        // Defining cluster configuration.
        IgniteConfiguration cfg = new IgniteConfiguration();
        
        // Defining Person cache configuration.
        CacheConfiguration<Integer, Person> personCfg = new CacheConfiguration<Integer, Person>("Person");

        personCfg.setBackups(1);

        // Group the cache belongs to.
        personCfg.setGroupName("group1");

        // Defining Organization cache configuration.
        CacheConfiguration orgCfg = new CacheConfiguration("Organization");

        orgCfg.setBackups(1);

        // Group the cache belongs to.
        orgCfg.setGroupName("group1");

        cfg.setCacheConfiguration(personCfg, orgCfg);

        // Starting the node.
        Ignition.start(cfg);
        // end::cfg[]
        Ignition.ignite().close();
    }

    @Test
    void partitionLossPolicy() {
        //tag::partition-loss-policy[]

        CacheConfiguration<Integer, Person> personCfg = new CacheConfiguration<Integer, Person>("Person");
        
        personCfg.setPartitionLossPolicy(PartitionLossPolicy.IGNORE);

        //end::partition-loss-policy[]
    }
}
