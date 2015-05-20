/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.ignite.scalar.examples.servicegrid

import org.apache.ignite.examples.servicegrid.{SimpleMapService, SimpleMapServiceImpl}
import org.apache.ignite.examples.{ExampleNodeStartup, ExamplesUtils}
import org.apache.ignite.lang.IgniteCallable
import org.apache.ignite.resources.ServiceResource
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._
import org.apache.ignite.{Ignite, Ignition}

import java.util.{Collection => JavaCollection}

import scala.collection.JavaConversions._

/**
 * Example that demonstrates how to deploy distributed services in Ignite.
 * Distributed services are especially useful when deploying singletons on the ignite,
 * be that cluster-singleton, or per-node-singleton, etc...
 * <p>
 * To start remote nodes, you must run [[ExampleNodeStartup]] in another JVM
 * which will start node with `examples/config/example-ignite.xml` configuration.
 * <p>
 * NOTE:<br/>
 * Starting `ignite.sh` directly will not work, as distributed services
 * cannot be peer-deployed and classes must be on the classpath for every node.
 */
object ScalarServicesExample extends App{
    /** Configuration file name. */
    private val CONFIG = "examples/config/example-ignite.xml"

    // Mark this node as client node.
    Ignition.setClientMode(true)

    scalar(CONFIG) {
        if (ExamplesUtils.hasServerNodes(ignite$)) {
            val svcs = ignite$.services(cluster$.forServers)

            try {
                svcs.deployClusterSingleton("myClusterSingletonService", new SimpleMapServiceImpl)

                svcs.deployNodeSingleton("myNodeSingletonService", new SimpleMapServiceImpl)

                svcs.deployMultiple("myMultiService", new SimpleMapServiceImpl, 2, 0)

                serviceProxyExample(ignite$)

                serviceInjectionExample(ignite$)
            }
            finally {
                ignite$.services.cancelAll()
            }
        }
    }

    /**
     * Simple example to demonstrate service proxy invocation of a remotely deployed service.
     *
     * @param ignite Ignite instance.
     * @throws Exception If failed.
     */
    @throws(classOf[Exception])
    private def serviceProxyExample(ignite: Ignite) {
        println(">>>")
        println(">>> Starting service proxy example.")
        println(">>>")

        val mapSvc: SimpleMapService[Integer, String] = ignite.services.serviceProxy("myNodeSingletonService",
            classOf[SimpleMapService[_, _]], true)

        val cnt = 10

        for (i <- 0 until cnt)
            mapSvc.put(i, Integer.toString(i))

        val mapSize = mapSvc.size

        println("Map service size: " + mapSize)

        if (mapSize != cnt) throw new Exception("Invalid map size [expected=" + cnt + ", actual=" + mapSize + ']')
    }

    /**
     * Simple example to demonstrate how to inject service proxy into distributed closures.
     *
     * @param ignite Ignite instance.
     * @throws Exception If failed.
     */
    @throws(classOf[Exception])
    private def serviceInjectionExample(ignite: Ignite) {
        println(">>>")
        println(">>> Starting service injection example.")
        println(">>>")

        val mapSvc: SimpleMapService[Integer, String] = ignite.services.serviceProxy("myClusterSingletonService",
            classOf[SimpleMapService[_, _]], true)

        val cnt = 10

        for (i <- 0 until cnt)
            mapSvc.put(i, Integer.toString(i))

        val mapSizes: JavaCollection[Integer] = ignite.compute.broadcast(new SimpleClosure)

        println("Closure execution result: " + mapSizes)


        for (mapSize <- mapSizes) if (mapSize != cnt) throw new Exception("Invalid map size [expected=" + cnt +
            ", actual=" + mapSize + ']')
    }

    /**
     * Simple closure to demonstrate auto-injection of the service proxy.
     */
    private class SimpleClosure extends IgniteCallable[Integer] {
        @ServiceResource(serviceName = "myClusterSingletonService", proxyInterface = classOf[SimpleMapService[_, _]])
        @transient
        private val mapSvc: SimpleMapService[_, _] = null

        @throws(classOf[Exception])
        def call: Integer = {
            val mapSize = mapSvc.size

            println("Executing closure [mapSize=" + mapSize + ']')

            mapSize
        }
    }
}
