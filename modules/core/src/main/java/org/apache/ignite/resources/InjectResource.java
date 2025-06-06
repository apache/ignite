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
 * Annotates a field or a setter method for injection of resource external to Ignite itself.
 * Use it whenever you would like to inject resources specified by your application.
 * <p>
 * Injections can be done into instances of following classes:
 * <ul>
 * <li>{@link org.apache.ignite.compute.ComputeTask}</li>
 * <li>{@link org.apache.ignite.compute.ComputeJob}</li>
 * <li>{@link org.apache.ignite.spi.IgniteSpi}</li>
 * <li>{@link org.apache.ignite.lifecycle.LifecycleBean}</li>
 * </ul>
 * <p>
 * <h1 class="header">Resource lookup</h1>
 * Either resource name or resource type will be used to locate injectable resources by name, or type.
 * Note that, runtime behavior might differ between various sources of injections. Some of them might
 * support lookup by name, some by type, some will be able to handle both.
 * <p>
 * Because looked up resources will be injected into a {@link Serializable} classes, they must
 * be declared as {@code transient}. This will avoid side effects when serialized form of object
 * passes source process boundaries.
 * <p>
 * The lifecycle of injected resources is controlled by injection container, not Ignite.
 * <p>
 * <h1 class="header">Examples</h1>
 * Here is how injection would typically happen:
 * <pre name="code" class="java">
 * public class MyGridJob implements ComputeJob {
 *      ...
 *      &#64;IgniteInject(resourceName = "bean-name")
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
 *     &#64;IgniteInject(resourceName = "bean-name")
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
 *     &#64;IgniteInject(resourceName = "bean-name")
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
 * where resource class can look like this:
 * <pre name="code" class="java">
 * public class MyUserBean {
 *     ...
 * }
 * </pre>
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.FIELD})
public @interface InjectResource {
    /**
     * Resource bean name in provided to look up
     * a Spring bean.
     *
     * @return Resource bean name.
     */
    String resourceName() default "";

    /**
     * Resource bean class in provided {@code ApplicationContext} to look up
     * a Spring bean.
     *
     * @return Resource bean class.
     */
    Class<?> resourceClass() default DEFAULT.class;

    /**
     * Determines whether the injection procedure should fail in case no beans with specified name or type are present
     * in the Spring Context.
     *
     * @return Whether absence of the bean with specified parameters will result in {@code NoSuchBeanDefinitionException}.
     */
    boolean required() default true;

    /** Dummy class to compensate for impossibility of having default null value for annotation method. */
    final class DEFAULT {
        // No-op.
    }
}
