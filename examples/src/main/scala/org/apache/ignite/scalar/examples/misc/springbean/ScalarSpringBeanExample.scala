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

package org.apache.ignite.scalar.examples.misc.springbean

import java.util.concurrent.Callable
import java.util.{ArrayList => JavaArrayList, Collection => JavaCollection}

import org.apache.ignite.Ignite
import org.apache.ignite.examples.ExampleNodeStartup
import org.apache.ignite.internal.util.scala.impl
import org.springframework.context.support.ClassPathXmlApplicationContext

/**
 * Demonstrates a simple use of Ignite configured with Spring.
 * <p>
 * String "Hello World." is printed out by Callable passed into
 * the executor service provided by Ignite. This statement could be printed
 * out on any node in the cluster.
 * <p>
 * The major point of this example is to show ignite injection by Spring
 * framework. Ignite bean is described in `spring-bean.xml` file and instantiated
 * by Spring context. Once application completed its execution Spring will
 * apply ignite bean destructor and stop the ignite.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: `'ignite.{sh|bat} examples/config/example-ignite.xml'`.
 * <p>
 * Alternatively you can run [[ExampleNodeStartup]] in another JVM which will start node
 * with `examples/config/example-ignite.xml` configuration.
 */
object ScalarSpringBeanExample extends App {
    println()
    println(">>> Spring bean example started.")

    // Initialize Spring factory.
    val ctx = new ClassPathXmlApplicationContext("org/apache/ignite/examples/misc/springbean/spring-bean.xml")

    try {
        val ignite = ctx.getBean("mySpringBean").asInstanceOf[Ignite]

        val exec = ignite.executorService

        val res = exec.submit(new Callable[String] {
            @impl def call(): String = {
                println("Hello world!")

                null
            }
        })

        res.get

        println(">>>")
        println(">>> Finished executing Ignite \"Spring bean\" example.")
        println(">>> You should see printed out of 'Hello world' on one of the nodes.")
        println(">>> Check all nodes for output (this node is also part of the cluster).")
        println(">>>")
    }
    finally {
        ctx.destroy()
    }
}
