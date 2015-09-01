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
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.gridify.aop.GridifyDefaultTask;

/**
 * {@code Gridify} annotation is the main way to grid-enable existing code.
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
 * By default, when neither {@link #taskClass()} or {@link #taskName()} are specified
 * a method with {@code @Gridify} annotation will be executed on randomly picked remote node.
 * <p>
 * Note that when using {@code @Gridify} annotation with default task (without
 * specifying explicit grid task), the state of the whole instance will be
 * serialized and sent out to remote node. Therefore the class must implement
 * {@link Serializable} interface. If you cannot make the class {@code Serializable},
 * then you must implement custom grid task which will take care of proper state
 * initialization. In either case, Ignite must be able to serialize the state passed to remote node.
 * <p>
 * Refer to {@link org.apache.ignite.compute.ComputeTask} documentation for more information on how a task
 * can be split into multiple sub-jobs.
 * <p>
 * <h1 class="header">Java Example</h1>
 * Here is a simple example how to grid-enable a Java method. The method {@code sayIt}
 * with {@code @Gridify} annotation will be executed on remote node.
 * <pre name="code" class="java">
 * ...
 * &#64;Gridify
 * public static void sayIt(String arg) {
 *    // Simply print out the argument.
 *    System.out.println(arg);
 * }
 * ...
 * </pre>
 * Here is an example of how to grid-enable a Java method with custom task. The custom task
 * logic will provide a way to split and aggregate the result. The method {@code sayIt} will
 * be executed on remote node.
 * <pre name="code" class="java">
 * public class GridifyHelloWorldTaskExample {
 *     ...
 *     &#64;Gridify(taskClass = GridifyHelloWorldTask.class, timeout = 3000)
 *     public static integer sayIt(String arg) {
 *         // Simply print out the argument.
 *         System.out.println(">>> Printing '" + arg + "' on this node from grid-enabled method.");
 *
 *         return arg.length();
 *     }
 *     ...
 * }
 * </pre>
 * The custom task will actually take the String passed into {@code sayIt(String)} method,
 * split it into words and execute every word on different remote node.
 * <pre name="code" class="java">
 * public class GridifyHelloWorldTask extends GridifyTaskSplitAdapter&lt;Integer&gt; {
 *    &#64;Override public Integer apply(String e) {
 *        return F.outJobs(F.yield(((String)arg.getMethodParameters()[0]).split(" "), new C1&lt;String, Integer&gt;() {
 *            &#64;Override public Integer apply(String e) {
 *                // Note that no recursive cross-cutting will happen here.
 *                return GridifyHelloWorldTaskExample.sayIt(e);
 *            }
 *        }));
 *    }
 *
 *    public Integer reduce(List&lt;GridComputeJobResult&gt; results) throws IgniteCheckedException {
 *        return results.size() - 1 + F.sum(F.&lt;Integer&gt;jobResults(results));
 *    }
 * }
 * </pre>
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
 * any specific runtime parameters for online weaving.
 * <p>
 * Note that this method of weaving is rather inconvenient and AspectJ or JBoss AOP is
 * recommended over it. Spring AOP can be used in situation when code augmentation is
 * undesired and cannot be used. It also allows for very fine grained control of what gets
 * weaved.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface Gridify {
    /**
     * Optional gridify task name. Note that either this name or {@link #taskClass()} must
     * be specified - but not both. If neither one is specified tasks' fully qualified name
     * will be used as a default name.
     */
    @SuppressWarnings({"JavaDoc"})
    String taskName() default "";

    /**
     * Optional gridify task class. Note that either this name or {@link #taskName()} must
     * be specified - but not both. If neither one is specified tasks' fully qualified name
     * will be used as a default name.
     */
    @SuppressWarnings({"JavaDoc"})
    Class<? extends ComputeTask<GridifyArgument, ?>> taskClass() default GridifyDefaultTask.class;

    /**
     * Optional gridify task execution timeout. Default is {@code 0}
     * which indicates that task will not timeout.
     */
    @SuppressWarnings({"JavaDoc"})
    long timeout() default 0;

    /**
     * Optional interceptor class. Since {@code null} are not supported the value of
     * {@code GridifyInterceptor.class} acts as a default value.
     */
    @SuppressWarnings({"JavaDoc"})
    Class<? extends GridifyInterceptor> interceptor() default GridifyInterceptor.class;

    /**
     * Name of the grid to use. By default, no-name default grid is used.
     * Refer to {@link org.apache.ignite.Ignition} for information about named grids.
     */
    @SuppressWarnings({"JavaDoc"})
    String gridName() default "";
}