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

import org.apache.ignite.lang.IgniteExperimental;

/**
 * Service call context.
 * <p>
 * This context is implicitly passed to the service and can be retrieved inside the service using {@link
 * ServiceContext#currentCallContext()}. It is accessible only from the local thread during the execution of a service
 * method.
 * <p>
 * Use {@link ServiceCallContext#builder()} to create the context builder.
 * <p>
 * <b>Note</b>: passing the context to the service may lead to performance overhead, so it should only be used for
 * "middleware" tasks.
 * <p>
 * Usage example:
 * <pre name="code" class="java">
 *
 * // Service implementation.
 * class HelloServiceImpl implements HelloService {
 *     &#64;ServiceContextResource
 *     ServiceContext ctx;
 *
 *     public String call(Strig msg) {
 *         return msg + ctx.currentCallContext().attribute("user");
 *     }
 *     ...
 * }
 * ...
 *
 * // Call this service with context.
 * ServiceCallContext callCtx = ServiceCallContext.builder().put("user", "John").build();
 * HelloService helloSvc = ignite.services().serviceProxy("hello-service", HelloService.class, false, callCtx, 0);
 * // Print "Hello John".
 * System.out.println( helloSvc.call("Hello ") );
 * </pre>
 *
 * @see ServiceContext
 * @see ServiceCallContextBuilder
 * @see ServiceCallInterceptor
 */
@IgniteExperimental
public interface ServiceCallContext {
    /**
     * Create a context builder.
     *
     * @return Context builder.
     */
    public static ServiceCallContextBuilder builder() {
        return new ServiceCallContextBuilder();
    }

    /**
     * Get the string attribute.
     *
     * @param name Attribute name.
     * @return String attribute value.
     */
    public String attribute(String name);

    /**
     * Get the binary attribute.
     *
     * @param name Attribute name.
     * @return Binary attribute value.
     */
    public byte[] binaryAttribute(String name);
}
