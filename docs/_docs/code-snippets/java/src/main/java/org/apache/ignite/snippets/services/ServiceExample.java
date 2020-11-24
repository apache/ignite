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
package org.apache.ignite.snippets.services;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.services.ServiceConfiguration;
import org.junit.jupiter.api.Test;

public class ServiceExample {

    @Test
    void serviceExample() {
        //tag::start-with-method[]
        Ignite ignite = Ignition.start();

        //get the services interface associated with all server nodes
        IgniteServices services = ignite.services();

        //start a node singleton
        services.deployClusterSingleton("myCounterService", new MyCounterServiceImpl());
        //end::start-with-method[]

        //tag::access-service[]
        //access the service by name
        MyCounterService counterService = ignite.services().serviceProxy("myCounterService",
                MyCounterService.class, false); //non-sticky proxy

        //call a service method
        counterService.increment();
        //end::access-service[]

        // Print the latest counter value from our counter service.
        System.out.println("Incremented value : " + counterService.get());
        
        //tag::undeploy[]
        services.cancel("myCounterService");
        //end::undeploy[]
        
        ignite.close();
    }

    @Test
    void deployWithClusterGroup() {
        //tag::deploy-with-cluster-group[]
        Ignite ignite = Ignition.start();

        //deploy the service to the nodes that host the cache named "myCache" 
        ignite.services(ignite.cluster().forCacheNodes("myCache"));

        //end::deploy-with-cluster-group[]
        ignite.close();
    }

    //tag::node-filter[]
    public static class ServiceFilter implements IgnitePredicate<ClusterNode> {
        @Override
        public boolean apply(ClusterNode node) {
            // The service will be deployed on the server nodes
            // that have the 'west.coast.node' attribute.
            return !node.isClient() && node.attributes().containsKey("west.coast.node");
        }
    }
    //end::node-filter[]

    @Test
    void affinityKey() {
        
        //tag::deploy-by-key[]
        Ignite ignite = Ignition.start();

        //making sure the cache exists
        ignite.getOrCreateCache("orgCache");

        ServiceConfiguration serviceCfg = new ServiceConfiguration();

        // Setting service instance to deploy.
        serviceCfg.setService(new MyCounterServiceImpl());

        // Setting service name.
        serviceCfg.setName("serviceName");
        serviceCfg.setTotalCount(1);

        // Specifying the cache name and key for the affinity based deployment.
        serviceCfg.setCacheName("orgCache");
        serviceCfg.setAffinityKey(123);

        IgniteServices services = ignite.services();

        // Deploying the service.
        services.deploy(serviceCfg);
        //end::deploy-by-key[]
        ignite.close();
    }

    @Test
    void deployingWithNodeFilter() {

        System.setProperty("west.coast.node", "true");

        //tag::deploy-with-node-filter[]
        Ignite ignite = Ignition.start();

        ServiceConfiguration serviceCfg = new ServiceConfiguration();

        // Setting service instance to deploy.
        serviceCfg.setService(new MyCounterServiceImpl());
        serviceCfg.setName("serviceName");
        serviceCfg.setMaxPerNodeCount(1);

        // Setting the nodes filter.
        serviceCfg.setNodeFilter(new ServiceFilter());

        // Getting an instance of IgniteService.
        IgniteServices services = ignite.services();

        // Deploying the service.
        services.deploy(serviceCfg);
        //end::deploy-with-node-filter[]
        ignite.close();
    }

    @Test
    void startWithConfig() {
        //tag::start-with-service-config[]
        Ignite ignite = Ignition.start();

        ServiceConfiguration serviceCfg = new ServiceConfiguration();

        serviceCfg.setName("myCounterService");
        serviceCfg.setMaxPerNodeCount(1);
        serviceCfg.setTotalCount(1);
        serviceCfg.setService(new MyCounterServiceImpl());

        ignite.services().deploy(serviceCfg);
        //end::start-with-service-config[]

        ignite.close();
    }

    @Test
    void serviceConfiguration() {
        //tag::service-configuration[]
        ServiceConfiguration serviceCfg = new ServiceConfiguration();

        serviceCfg.setName("myCounterService");
        serviceCfg.setMaxPerNodeCount(1);
        serviceCfg.setTotalCount(1);
        serviceCfg.setService(new MyCounterServiceImpl());

        IgniteConfiguration igniteCfg = new IgniteConfiguration()
                .setServiceConfiguration(serviceCfg);

        // Start the node.
        Ignite ignite = Ignition.start(igniteCfg);
        //end::service-configuration[]
        ignite.close();
    }
}
