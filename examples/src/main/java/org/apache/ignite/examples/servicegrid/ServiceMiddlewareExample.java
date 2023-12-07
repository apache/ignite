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

package org.apache.ignite.examples.servicegrid;

import java.util.Date;
import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.apache.ignite.examples.ExamplesUtils;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.services.ServiceCallContext;
import org.apache.ignite.services.ServiceCallInterceptor;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceContext;

/**
 * The example shows how to add a middleware layer for distributed services in Ignite.
 * <p>
 * To start remote nodes, run {@link ExampleNodeStartup} in another JVM. It will start
 * a node with {@code examples/config/example-ignite.xml} configuration.
 * <p>
 * NOTE:<br/>
 * Starting {@code ignite.sh} directly will not work, as distributed services (and interceptors)
 * cannot be peer-deployed and classes must be in the classpath for each node.
 */
public class ServiceMiddlewareExample {
    /** Service name. */
    private static final String SVC_NAME = "MapService";

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws Exception If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        // Mark this node as client node.
        Ignition.setClientMode(true);

        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            if (!ExamplesUtils.hasServerNodes(ignite))
                return;

            ServiceCallInterceptor audit = new Audit();
            ServiceCallInterceptor access = (mtd, args0, ctx, next) -> {
                ServiceCallContext callCtx = ctx.currentCallContext();

                if (callCtx == null || callCtx.attribute("user") == null || callCtx.attribute("user").isEmpty())
                    throw new SecurityException("Anonymous access is restricted.");

                return next.call();
            };

            ServiceConfiguration cfg = new ServiceConfiguration()
                .setName(SVC_NAME)
                .setService(new SimpleMapServiceImpl<>())
                .setTotalCount(1)
                .setInterceptors(audit, access);

            try {
                ignite.services().deploy(cfg);

                // Create proxy without context.
                SimpleMapService<Object, Object> proxy =
                    ignite.services().serviceProxy(SVC_NAME, SimpleMapService.class, false);

                try {
                    System.out.println("Try to call the proxy method without context.");
                    // The method call will be intercepted with a SecurityException because no username was provided.
                    proxy.put(0, 0);
                }
                catch (IgniteException expected) {
                    expected.printStackTrace();
                }

                // Create a service call context with the username.
                ServiceCallContext callCtx = ServiceCallContext.builder().put("user", "John").build();

                // Bind it to the service proxy.
                proxy = ignite.services().serviceProxy(SVC_NAME, SimpleMapService.class, false, callCtx);

                System.out.println("Call the proxy method with context.");

                for (int i = 0; i < 10; i++)
                    proxy.put(i, i);

                for (int i = 0; i < 5; i++)
                    proxy.get(i * 2);

                System.out.println("Map size: " + proxy.size());
            }
            finally {
                ignite.services().cancelAll();
            }
        }
    }

    /** */
    private static class Audit implements ServiceCallInterceptor {
        /** Serial version UID. */
        private static final long serialVersionUID = 0L;

        /** Injected Ignite logger. */
        @LoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public Object invoke(String mtd, Object[] args, ServiceContext ctx, Callable<Object> next) throws Exception {
            String serviceName = ctx.name();
            ServiceCallContext callCtx = ctx.currentCallContext();
            String user = callCtx == null ? null : callCtx.attribute("user");

            recordEvent(user, serviceName, mtd, "start");

            try {
                // Execute service method.
                Object res = next.call();

                // Record finish event after execution of the service method.
                recordEvent(user, serviceName, mtd, "result " + res);

                return res;
            }
            catch (Exception e) {
                log.error("Intercepted error", e);

                // Record error.
                recordEvent(user, serviceName, mtd, "error: " + e.getMessage());

                // Re-throw exception to initiator.
                throw e;
            }
        }

        /**
         * Record an event to current server node console output.
         */
        private void recordEvent(String user, String svc, String mtd, String msg) {
            System.out.printf("[%tT][%s][%s][%s] %s%n", new Date(), user, svc, mtd, msg);
        }
    }
}
