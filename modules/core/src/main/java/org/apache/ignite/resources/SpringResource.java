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

import java.io.Serializable;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates a field or a setter method for injection of resource
 * from Spring {@code ApplicationContext}. Use it whenever you would
 * like to inject resources specified in Spring application context of XML
 * configuration.
 * <p>
 * Logger can be injected into instances of following classes:
 * <ul>
 * <li>{@link org.apache.ignite.compute.ComputeTask}</li>
 * <li>{@link org.apache.ignite.compute.ComputeJob}</li>
 * <li>{@link org.apache.ignite.spi.IgniteSpi}</li>
 * <li>{@link org.apache.ignite.lifecycle.LifecycleBean}</li>
 * </ul>
 * <p>
 * <h1 class="header">Resource Name</h1>
 * This is a mandatory parameter. Resource name will be used to access
 * Spring resources from Spring {@code ApplicationContext} or XML configuration.
 * <p>
 * Note that Spring resources cannot be peer-class-loaded. They must be available in
 * every {@code ApplicationContext} or Spring XML configuration on every grid node.
 * For this reason, if injected into a {@link Serializable} class, they must
 * be declared as {@code transient}.
 * <p>
 * The lifecycle of Spring resources is controlled by Spring container.
 * <p>
 * <h1 class="header">Examples</h1>
 * Here is how injection would typically happen:
 * <pre name="code" class="java">
 * public class MyGridJob implements ComputeJob {
 *      ...
 *      &#64;SpringResource(resourceName = "bean-name")
 *      private transient MyUserBean rsrc;
 *      ...
 *  }
 * </pre>
 * or
 * <pre name="code" class="java">
 * public class MyGridJob implements ComputeJob {
 *     ...
 *     private transient MyUserBean rsrc;
 *     ...
 *     &#64;SpringResource(resourceName = "bean-name")
 *     public void setMyUserBean(MyUserBean rsrc) {
 *          this.rsrc = rsrc;
 *     }
 *     ...
 * }
 * </pre>
 * and user resource {@code MyUserResource}
 * <pre name="code" class="java">
 * public class MyUserResource {
 *     ...
 *     &#64;SpringResource(resourceName = "bean-name")
 *     private MyUserBean rsrc;
 *     ...
 *     // Inject logger (or any other resource).
 *     &#64;LoggerResource
 *     private IgniteLogger log;
 *
 *     // Inject ignite instance (or any other resource).
 *     &#64;IgniteInstanceResource
 *     private Ignite ignite;
 *     ...
 * }
 * </pre>
 * where spring bean resource class can look like this:
 * <pre name="code" class="java">
 * public class MyUserBean {
 *     ...
 * }
 * </pre>
 * and Spring file
 * <pre name="code" class="xml">
 * &lt;bean id="bean-name" class="my.foo.MyUserBean" singleton="true"&gt;
 *         ...
 * &lt;/bean&gt;
 * </pre>
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.FIELD})
public @interface SpringResource {
    /**
     * Resource bean name in provided {@code ApplicationContext} to look up
     * a Spring bean.
     *
     * @return Resource bean name.
     */
    String resourceName();
}