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
 * Annotates a field or a setter method for injection of current {@link org.apache.ignite.Ignite} instance.
 * It can be injected into grid tasks and grid jobs. Note that grid instance will
 * not be injected into SPI's, as there is no grid during SPI start.
 * <p>
 * Grid instance can be injected into instances of following classes:
 * <ul>
 * <li>{@link org.apache.ignite.compute.ComputeTask}</li>
 * <li>{@link org.apache.ignite.compute.ComputeJob}</li>
 * <li>{@link org.apache.ignite.lifecycle.LifecycleBean}</li>
 * <li>All closures and predicates that can run on grid.</li>
 * </ul>
 * <p>
 * Here is how injection would typically happen:
 * <pre name="code" class="java">
 * public class MyIgniteJob implements ComputeJob {
 *      ...
 *      &#64;IgniteInstanceResource
 *      private Ignite ignite;
 *      ...
 *  }
 * </pre>
 * or
 * <pre name="code" class="java">
 * public class MyIgniteJob implements ComputeJob {
 *     ...
 *     private Ignite ignite;
 *     ...
 *     &#64;IgniteInstanceResource
 *     public void setIgnite(Ignite ignite) {
 *          this.ignite = ignite;
 *     }
 *     ...
 * }
 * </pre>
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.FIELD})
public @interface IgniteInstanceResource {
    // No-op.
}