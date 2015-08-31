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

package org.apache.ignite.resources;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates a field or a setter method for injection of Ignite service(s) by specified service name.
 * If more than one service is deployed on a server, then the first available instance will be returned.
 * <p>
 * Here is how injection would typically happen:
 * <pre name="code" class="java">
 * public class MyGridJob implements ComputeJob {
 *      ...
 *      // Inject single instance of 'myService'. If there is
 *      // more than one, first deployed instance will be picked.
 *      &#64;IgniteServiceResource(serviceName = "myService", proxyInterface = MyService.class)
 *      private MyService svc;
 *      ...
 *  }
 * </pre>
 * or attach the same annotations to methods:
 * <pre name="code" class="java">
 * public class MyGridJob implements ComputeJob {
 *     ...
 *     private MyService svc;
 *     ...
 *      // Inject all locally deployed instances of 'myService'.
 *     &#64;IgniteServiceResource(serviceName = "myService")
 *     public void setMyService(MyService svc) {
 *          this.svc = svc;
 *     }
 *     ...
 * }
 * </pre>
 * <p>
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.FIELD})
public @interface ServiceResource {
    /**
     * Service name.
     *
     * @return Name of the injected services.
     */
    public String serviceName();

    /**
     * In case if an instance of the service is not available locally,
     * an instance of the service proxy for a remote service instance
     * may be returned. If you wish to return only locally deployed
     * instance, then leave this property as {@code null}.
     * <p>
     * For more information about service proxies, see
     * {@link org.apache.ignite.IgniteServices#serviceProxy(String, Class, boolean)} documentation.
     *
     * @return Interface class for remote service proxy.
     */
    public Class<?> proxyInterface() default Void.class;

    /**
     * Flag indicating if a sticky instance of a service proxy should be returned.
     * This flag is only valid if {@link #proxyInterface()} is not {@code null}.
     * <p>
     * For information about sticky flag, see {@link org.apache.ignite.IgniteServices#serviceProxy(String, Class, boolean)}
     * documentation.
     *
     * @return {@code True} if a sticky instance of a service proxy should be injected.
     */
    public boolean proxySticky() default false;
}