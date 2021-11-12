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
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;

/**
 * Annotates a field or a setter method for injecting a {@link ServiceContext service context} into a {@link Service
 * service} instance.
 * <p>
 * It is guaranteed that context will be injected before calling the {@link Service#init()} method.
 * <p>
 * Here is how injection would typically happen:
 * <pre name="code" class="java">
 * public class MyServiceImpl implements MyService {
 *      ...
 *      &#64;ServiceContextResource
 *      private ServiceContext ctx;
 *      ...
 * }
 * </pre>
 * or attach the same annotation to the method:
 * <pre name="code" class="java">
 * public class MyServiceImpl implements MyService {
 *     ...
 *     private ServiceContext ctx;
 *     ...
 *     &#64;ServiceContextResource
 *     public void setServiceContext(ServiceContext ctx) {
 *          this.ctx = ctx;
 *     }
 *     ...
 * }
 * </pre>
 *
 * @see Service
 * @see ServiceContext
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.FIELD})
public @interface ServiceContextResource {
    // No-op.
}
