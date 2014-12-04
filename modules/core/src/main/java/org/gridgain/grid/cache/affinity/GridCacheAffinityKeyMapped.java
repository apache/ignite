/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.affinity;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.spi.loadbalancing.*;

import java.lang.annotation.*;
import java.util.concurrent.*;

/**
 * Optional annotation to specify custom key-to-node affinity. Affinity key is a key
 * which will be used to determine a node on which given cache key will be stored. This
 * annotation allows to mark a field or a method in the cache key object that will be
 * used as an affinity key (instead of the entire cache key object that is used for
 * affinity by default). Note that a class can have only one field or method annotated
 * with {@code @GridCacheAffinityKeyMapped} annotation.
 * <p>
 * One of the major use cases for this annotation is the routing of grid computations
 * to the nodes where the data for this computation is cached, the concept
 * otherwise known as {@code Collocation Of Computations And Data}.
 * <p>
 * <h1 class="header">Mapping Cache Keys</h1>
 * The default implementation of {@link GridCacheAffinityKeyMapper}, which will be used
 * if no explicit affinity mapper is specified in cache configuration, will first look
 * for any field or method annotated with {@code @GridCacheAffinityKeyMapped} annotation.
 * If such field or method is not found, then the cache key itself will be used for
 * key-to-node affinity (this means that all objects with the same cache key will always
 * be routed to the same node). If such field or method is found, then the value of this
 * field or method will be used for key-to-node affinity. This allows to specify alternate
 * affinity key, other than the cache key itself, whenever needed.
 * <p>
 * For example, if a {@code Person} object is always accessed together with a {@code Company} object
 * for which this person is an employee, then for better performance and scalability it makes sense to
 * collocate {@code Person} objects together with their {@code Company} object when storing them in
 * cache. To achieve that, cache key used to cache {@code Person} objects should have a field or method
 * annotated with {@code @GridCacheAffinityKeyMapped} annotation, which will provide the value of
 * the company key for which that person works, like so:
 * <pre name="code" class="java">
 * public class PersonKey {
 *     // Person ID used to identify a person.
 *     private String personId;
 *
 *     // Company ID which will be used for affinity.
 *     &#64;GridCacheAffinityKeyMapped
 *     private String companyId;
 *     ...
 * }
 * ...
 * // Instantiate person keys.
 * Object personKey1 = new PersonKey("myPersonId1", "myCompanyId");
 * Object personKey2 = new PersonKey("myPersonId2", "myCompanyId");
 *
 * // Both, the company and the person objects will be cached on the same node.
 * cache.put("myCompanyId", new Company(..));
 * cache.put(personKey1, new Person(..));
 * cache.put(personKey2, new Person(..));
 * </pre>
 * <p>
 * <h2 class="header">GridCacheAffinityKey</h2>
 * For convenience, you can also optionally use {@link GridCacheAffinityKey} class. Here is how a
 * {@code PersonKey} defined above would look using {@link GridCacheAffinityKey}:
 * <pre name="code" class="java">
 * Object personKey1 = new GridCacheAffinityKey("myPersonId1", "myCompanyId");
 * Object personKey2 = new GridCacheAffinityKey("myPersonId2", "myCompanyId");
 *
 * // Both, the company and the person objects will be cached on the same node.
 * cache.put(myCompanyId, new Company(..));
 * cache.put(personKey1, new Person(..));
 * cache.put(personKey2, new Person(..));
 * </pre>
 * <p>
 * <h1 class="header">Collocating Computations And Data</h1>
 * It is also possible to route computations to the nodes where the data is cached. This concept
 * is otherwise known as {@code Collocation Of Computations And Data}. In this case,
 * {@code @GridCacheAffinityKeyMapped} annotation allows to specify a routing affinity key for a
 * {@link GridComputeJob} or any other grid computation, such as {@link Runnable}, {@link Callable}, or
 * {@link org.apache.ignite.lang.IgniteClosure}. It should be attached to a method or field that provides affinity key
 * for the computation. Only one annotation per class is allowed. Whenever such annotation is detected,
 * then {@link GridLoadBalancingSpi} will be bypassed, and computation will be routed to the grid node
 * where the specified affinity key is cached. You can also use optional {@link GridCacheName @GridCacheName}
 * annotation whenever non-default cache name needs to be specified.
 * <p>
 * Here is how this annotation can be used to route a job to a node where Person object
 * is cached with ID "1234":
 * <pre name="code" class="java">
 * G.grid().run(new Runnable() {
 *     // This annotation is optional. If omitted, then default
 *     // no-name cache will be used.
 *     &#64;GridCacheName
 *     private String cacheName = "myCache";
 *
 *     // This annotation specifies that computation should be routed
 *     // precisely to the node where key '1234' is cached.
 *     &#64;GridCacheAffinityKeyMapped
 *     private String personKey = "1234";
 *
 *     &#64;Override public void run() {
 *         // Some computation logic here.
 *         ...
 *     }
 * };
 * </pre>
 * The same can be achieved by annotating method instead of field as follows:
 * <pre name="code" class="java">
 * G.grid().run(new Runnable() {
 *     &#64;Override public void run() {
 *         // Some computation logic here.
 *         ...
 *     }
 *
 *     // This annotation is optional. If omitted, then default
 *     // no-name cache will be used.
 *     &#64;GridCacheName
 *     public String cacheName() {
 *         return "myCache";
 *     }
 *
 *     // This annotation specifies that computation should be routed
 *     // precisely to the node where key '1234' is cached.
 *     &#64;GridCacheAffinityKeyMapped
 *     public String personKey() {
 *         return "1234";
 *     }
 * };
 * </pre>
 * <p>
 * For more information about cache affinity also see {@link GridCacheAffinityKeyMapper} and
 * {@link GridCacheAffinityFunction} documentation.
 * Affinity for a key can be found from any node, regardless of whether it has cache started
 * or not. If cache is not started, affinity function will be fetched from the remote node
 * which does have the cache running.
 *
 * @see GridCacheName
 * @see GridCacheAffinityFunction
 * @see GridCacheAffinityKeyMapper
 * @see GridCacheAffinityKey
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD})
public @interface GridCacheAffinityKeyMapped {
    // No-op.
}
