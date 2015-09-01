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

package org.apache.ignite.examples.misc.springbean;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.apache.ignite.Ignite;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Demonstrates a simple use of Ignite configured with Spring.
 * <p>
 * String "Hello World." is printed out by Callable passed into
 * the executor service provided by Ignite. This statement could be printed
 * out on any node in the cluster.
 * <p>
 * The major point of this example is to show ignite injection by Spring
 * framework. Ignite bean is described in {@code spring-bean.xml} file and instantiated
 * by Spring context. Once application completed its execution Spring will
 * apply ignite bean destructor and stop the ignite.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-ignite.xml'}.
 * <p>
 * Alternatively you can run {@link ExampleNodeStartup} in another JVM which will start node
 * with {@code examples/config/example-ignite.xml} configuration.
 */
public final class SpringBeanExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws Exception If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        System.out.println();
        System.out.println(">>> Spring bean example started.");

        // Initialize Spring factory.
        ClassPathXmlApplicationContext ctx =
            new ClassPathXmlApplicationContext("org/apache/ignite/examples/misc/springbean/spring-bean.xml");

        try {
            // Get ignite from Spring (note that local cluster node is already started).
            Ignite ignite = (Ignite)ctx.getBean("mySpringBean");

            // Execute any method on the retrieved ignite instance.
            ExecutorService exec = ignite.executorService();

            Future<String> res = exec.submit(new Callable<String>() {
                @Override public String call() throws Exception {
                    System.out.println("Hello world!");

                    return null;
                }
            });

            // Wait for callable completion.
            res.get();

            System.out.println(">>>");
            System.out.println(">>> Finished executing Ignite \"Spring bean\" example.");
            System.out.println(">>> You should see printed out of 'Hello world' on one of the nodes.");
            System.out.println(">>> Check all nodes for output (this node is also part of the cluster).");
            System.out.println(">>>");
        }
        finally {
            // Stop local cluster node.
            ctx.destroy();
        }
    }
}