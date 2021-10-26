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

import org.apache.ignite.internal.processors.service.ServiceCallContextImpl;

/**
 * Service call context.
 * <p>
 * This context is implicitly passed to the service and can be retrieved inside the service using {@link
 * ServiceContext#currentCallContext()}. It is accessible only from the local thread during the execution of a service
 * method.
 * <p>
 * Use {@link ServiceCallContext#create()} to instantiate the context.
 * <p>
 * <b>Note</b>: passing the context to the service may lead to performance overhead, so it should only be used for
 * "middleware" tasks.
 * <p>
 * Usage example:
 * <pre name="code" class="java">
 *
 * // Service implementation.
 * class HelloServiceImpl implements HelloService {
 *     ServiceContext ctx;
 *
 *     public void init(ServiceContext ctx) {
 *         this.ctx = ctx;
 *     }
 *
 *     public String call(Strig msg) {
 *         return msg + ctx.currentCallContext().attribute("user");
 *     }
 *     ...
 * }
 * ...
 *
 * // Call this service with context.
 * ServiceCallContext callCtx = ServiceCallContext.create().put("user", "John");
 * HelloService helloSvc = ignite.services().serviceProxy("hello-service", HelloService.class, false, callCtx, 0);
 * // Print "Hello John".
 * System.out.println( helloSvc.call("Hello ") );
 * </pre>
 *
 * @see ServiceContext
 */
public interface ServiceCallContext {
    /**
     * Factory method for creating an internal implementation.
     *
     * @return Service call context.
     */
    public static ServiceCallContext create() {
        return new ServiceCallContextImpl();
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
    public byte[] binary(String name);

    /**
     * Put new string attribute.<br<
     * If the context previously contained a mapping for the key, the old value is replaced by the specified value.
     *
     * @param name Attribute name.
     * @param value Attribute value.
     * @return This for chaining.
     */
    public ServiceCallContext put(String name, String value);

    /**
     * Put new binary attribute.<br<
     * If the context previously contained a mapping for the key, the old value is replaced by the specified value.
     *
     * @param name Attribute name.
     * @param value Attribute value.
     * @return This for chaining.
     */
    public ServiceCallContext put(String name, byte[] value);
}
