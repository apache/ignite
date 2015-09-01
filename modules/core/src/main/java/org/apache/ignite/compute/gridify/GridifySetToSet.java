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

package org.apache.ignite.compute.gridify;

import java.io.Serializable;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * {@code GridifySetToSet} annotation allows to grid-enable existing code with specific semantics.
 * <p>
 * This annotation can be applied to any public method that needs to be grid-enabled,
 * static or non-static. When this annotation is applied to a method, the method
 * execution is considered to be grid-enabled. In general, the execution of this
 * method can be transferred to another node in the grid, potentially splitting it
 * into multiple subtasks to be executed in parallel on multiple grid nodes. But
 * from the caller perspective this method still looks and behaves like a local apply.
 * This is achieved by utilizing AOP-based interception of the annotated code and
 * injecting all the necessary grid logic into the method execution.
 * <p>
 * {@code GridifySetToSet} used as extended version of {@code Gridify} annotation.
 * It provides automated gridification of two popular types of mathematical
 * functions that allow for "embarrassingly" simple parallelization (for example,
 * find prime numbers in collection or find maximum in collection). The goal of this
 * annotation is to provide very simple and easy to use ForkJoin gridification
 * for some often used use cases. Note that in a standard {@code Gridify} annotation
 * the user has to write {@link org.apache.ignite.compute.ComputeTask} to provide ForkJoin behavior.
 * In a proposed design - the ForkJoin will be achieved <b>automatically</b>.
 * <p>
 * Let <b>F</b> be the function or a method in Java language and <b>St</b> be a set of values
 * of type {@code t} or a typed Collection&lt;T&gt; in Java. Let also {@code R} be
 * the result value. These are the types of functions that will be supported (defined in
 * terms of their properties):
 * <p>
 * {@code F (S 1, S 2, ..., S k) => {R 1, R 2, ..., R n,}, where F (S 1, S 2, ..., S k) == {F (S 1), F (S 2), ..., F (S k)}}
 * which defines a function whose result can be constructed as concatenation of results
 * of the same function applied to subsets of the original set.
 * <p>
 * In definition above we used Collection&lt;T&gt; for the purpose of example.
 * The following Java types and their subtypes will have to be supported as return values or method parameters:
 * <ul>
 * <li>java.util.Collection</li>
 * <li>java.util.Iterator</li>
 * <li>java.util.Enumeration</li>
 * <li>java.lang.CharSequence</li>
 * <li>java array</li>
 * </ul>
 * <p>
 * Note that when using {@code @GridifySetToSet} annotation the state of the whole instance will be
 * serialized and sent out to remote node. Therefore the class must implement
 * {@link Serializable} interface. If you cannot make the class {@code Serializable},
 * then you must implement custom grid task which will take care of proper state
 * initialization. In either case, Ignite must be able to serialize the state passed to remote node.
 * <p>
 * <h1 class="header">Java Example</h1>
 * <p>
 * Example for these types of functions would be any function that is looking for a subset of data
 * in a given input collection. For example:
 * <pre name="code" class="java">
 * ...
 * &#64;GridifySetToSet(threshold = 2)
 * public static Collection&lt;Number&gt; findAllPrimeNumbers(Collection&lt;Number&gt; input) {...}
 * ...
 * </pre>
 * This function searches all elements in the input collection and returns another collection
 * containing only prime numbers found in the original collection. We can split this collection into
 * two sub-collection, find primes in each - and then simply concatenate two collections to get
 * the final set of all primes.
 * <p>
 * The formal definition of (or requirement for) such method:
 * <ul>
 * <li>Have return value of type <b>Collection&lt;T&gt;</b></li>
 * <li>Have one and only one parameter of type <b>Collection&lt;T&gt;</b></li>
 * <li>Order of <b>Collection&lt;T&gt;</b> parameter is irrelevant</li>
 * <li>Method can have as many other parameters as needed of any type except for <b>Collection&lt;T&gt;</b></li>
 * <li>Logic of the method must allow for concatenation of results from the same method apply
 * on a subset of the original collection</li>
 * </ul>
 * <p>
 * <h1 class="header">Jboss AOP</h1>
 * The following configuration needs to be applied to enable JBoss byte code
 * weaving. Note that Ignite is not shipped with JBoss and necessary
 * libraries will have to be downloaded separately (they come standard
 * if you have JBoss installed already):
 * <ul>
 * <li>
 *      The following JVM configuration must be present:
 *      <ul>
 *      <li>{@code -javaagent:[path to jboss-aop-jdk50-4.x.x.jar]}</li>
 *      <li>{@code -Djboss.aop.class.path=[path to ignite.jar]}</li>
 *      <li>{@code -Djboss.aop.exclude=org,com -Djboss.aop.include=org.apache.ignite.examples}</li>
 *      </ul>
 * </li>
 * <li>
 *      The following JARs should be in a classpath:
 *      <ul>
 *      <li>{@code javassist-3.x.x.jar}</li>
 *      <li>{@code jboss-aop-jdk50-4.x.x.jar}</li>
 *      <li>{@code jboss-aspect-library-jdk50-4.x.x.jar}</li>
 *      <li>{@code jboss-common-4.x.x.jar}</li>
 *      <li>{@code trove-1.0.2.jar}</li>
 *      </ul>
 * </li>
 * </ul>
 * <p>
 * <h1 class="header">AspectJ AOP</h1>
 * The following configuration needs to be applied to enable AspectJ byte code
 * weaving.
 * <ul>
 * <li>
 *      JVM configuration should include:
 *      {@code -javaagent:${IGNITE_HOME}/libs/aspectjweaver-1.7.2.jar}
 * </li>
 * <li>
 *      META-INF/aop.xml file should be created and specified on the classpath.
 *      The file should contain Gridify aspects and needed weaver options.
 * </li>
 * </ul>
 * <p>
 * <h1 class="header">Spring AOP</h1>
 * Spring AOP framework is based on dynamic proxy implementation and doesn't require
 * any specific runtime parameters for online weaving. All weaving is on-demand and should
 * be performed by calling method
 * {@ignitelink org.apache.ignite.compute.gridify.aop.spring.GridifySpringEnhancer#enhance(java.lang.Object)} for the object
 * that has method with {@link GridifySetToSet} annotation.
 * <p>
 * Note that this method of weaving is rather inconvenient and AspectJ or JBoss AOP is
 * recommended over it. Spring AOP can be used in situation when code augmentation is
 * undesired and cannot be used. It also allows for very fine grained control of what gets
 * weaved.
 */
@SuppressWarnings({"JavaDoc"})
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface GridifySetToSet {
    /**
     * Optional node filter to filter nodes participated in job split.
     */
    Class<? extends GridifyNodeFilter> nodeFilter() default GridifyNodeFilter.class;

    /**
     * Optional gridify task execution timeout. Default is {@code 0}
     * which indicates that task will not timeout.
     */
    long timeout() default 0;

    /**
     * Optional parameter that defines the minimal value below which the
     * execution will NOT be grid-enabled.
     */
    int threshold() default 0;

    /**
     * Optional parameter that defines a split size. Split size in other words means how big will
     * be the sub-collection
     * that will travel to remote nodes. Note that this is NOT a dynamic setting and you have to set
     * the split size up front. This may look
     * as a deficiency but Ignite will properly handle this case by giving more than one sub-collection
     * to a specific node (as defined by load balancing SPI in use).
     * <p>
     * This value should be greater than zero and can be less, equal or greater than {@link #threshold()}
     * value. In most cases, however, the optimal value for the split size is the {@link #threshold()} value.
     * For example, if input collection size is 100, number of nodes 10 and split size is set to 5 - Ignition
     * will submit 2 sub-collections of 5 elements each to each node (provided in order by load
     * balancing SPI).
     * <p>
     * By default (when split size is zero) - Ignite will automatically determine the split size based on
     * number of nodes and collection size - if collection size is available (not an iterator). If collection
     * size cannot be determined - the split size will default to threshold. If threshold is not set - a runtime
     * exception will be thrown.
     */
    int splitSize() default 0;

    /**
     * Optional interceptor class.
     */
    Class<? extends GridifyInterceptor> interceptor() default GridifyInterceptor.class;

    /**
     * Name of the grid to use. By default, no-name default grid is used.
     * Refer to {@link org.apache.ignite.Ignition} for information about named grids.
     */
    String gridName() default "";
}