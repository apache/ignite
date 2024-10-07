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

package org.apache.ignite.services;

import java.io.Serializable;
import java.util.concurrent.Callable;
import org.apache.ignite.lang.IgniteExperimental;

/**
 * Service call interceptor.
 * <p>
 * Allows the user to intercept the call to any service method except lifecycle methods
 * ({@link Service#init init()}, {@link Service#execute() execute()} and {@link Service#cancel() cancel()}).
 * <p>
 * A typical use of an interceptor is a middleware logic that applies to all custom methods in a service.
 * The user can specify multiple interceptors in the {@link ServiceConfiguration service configuration}.
 * Each interceptor invokes the next interceptor in the chain using a delegated call, the last interceptor
 * will call the service method.
 * <p>
 * Usage example:
 * <pre name="code" class="java">
 * ServiceCallInterceptor security = (mtd, args, ctx, svcCall) -&gt; {
 *     if (!CustomSecurityProvider.get().access(mtd, ctx.currentCallContext().attribute("sessionId")))
 *         throw new SecurityException("Method invocation is not permitted");
 *
 *     // Execute remaining interceptors and service method.
 *     return svcCall.call();
 * };
 *
 * ServiceCallInterceptor audit = (mtd, args, ctx, svcCall) -&gt; {
 *     String sessionId = ctx.currentCallContext().attribute("sessionId");
 *     AuditProvider prov = AuditProvider.get();
 *
 *     // Record an event before execution of the method.
 *     prov.recordStartEvent(ctx.name(), mtd, sessionId);
 *
 *     try {
 *         // Execute service method.
 *         return svcCall.call();
 *     }
 *     catch (Exception e) {
 *         // Record error.
 *         prov.recordError(ctx.name(), mtd, sessionId), e.getMessage());
 *
 *         // Re-throw exception to initiator.
 *         throw e;
 *     }
 *     finally {
 *         // Record finish event after execution of the service method.
 *         prov.recordFinishEvent(ctx.name(), mtd, sessionId);
 *     }
 * }
 *
 * ServiceConfiguration svcCfg = new ServiceConfiguration()
 *     .setName("service")
 *     .setService(new MyServiceImpl())
 *     .setMaxPerNodeCount(1)
 *     .setInterceptors(audit, security);
 *
 * // Deploy service.
 * ignite.services().deploy(svcCfg);
 *
 * // Set context parameters for the service proxy.
 * ServiceCallContext callCtx = ServiceCallContext.builder().put("sessionId", sessionId).build();
 *
 * // Make a service proxy with the call context to define the "sessionId" attribute.
 * MyService proxy = ignite.services().serviceProxy("service", MyService.class, false, callCtx, 0);
 *
 * // Service method call will be intercepted.
 * proxy.placeOrder(order1);
 * proxy.placeOrder(order2);
 * </pre>
 *
 * @see ServiceCallContext
 * @see ServiceContext
 */
@IgniteExperimental
public interface ServiceCallInterceptor extends Serializable {
    /**
     * Intercepts delegated service call.
     *
     * @param mtd Method name.
     * @param args Method arguments.
     * @param ctx Service context.
     * @param next Delegated call to a service method and/or interceptor in the chain.
     * @return Service call result.
     */
    public Object invoke(String mtd, Object[] args, ServiceContext ctx, Callable<Object> next) throws Exception;
}
