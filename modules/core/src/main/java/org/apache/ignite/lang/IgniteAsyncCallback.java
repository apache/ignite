/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.lang;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListener;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.configuration.IgniteConfiguration;

/**
 * If callback has this annotation then it will be executing in another thread.
 * <p>
 * Currently this annotation is supported for:
 * <ol>
 *     <li>{@link ContinuousQuery} - {@link CacheEntryUpdatedListener} and {@link CacheEntryEventFilter}.</li>
 * </ol>
 * <p>
 * For example, if {@link CacheEntryEventFilter filter} or {@link CacheEntryListener}
 * has the annotation then callbacks will be executing to asyncCallback thread pool. It allows to use cache API
 * in a callbacks. This thread pool can be configured by {@link IgniteConfiguration#setAsyncCallbackPoolSize(int)}.
 * <h1 class="header">Example</h1>
 * As an example, suppose we have cache with {@code 'Person'} objects and we need
 * to query all persons with salary above then 1000. Also remote filter will update some entries.
 * <p>
 * Here is the {@code Person} class:
 * <pre name="code" class="java">
 * public class Person {
 *     // Name.
 *     private String name;
 *
 *     // Salary.
 *     private double salary;
 *
 *     ...
 * }
 * </pre>
 * <p>
 * Here is the {@code ExampleCacheEntryFilter} class:
 * <pre name="code" class="java">
 * &#064;IgniteAsyncCallback
 * public class ExampleCacheEntryFilter implements CacheEntryEventFilter&lt;Integer, Person&gt; {
 *     &#064;IgniteInstanceResource
 *     private Ignite ignite;
 *
 *     // Continuous listener will be notified for persons with salary above 1000.
 *     // Filter increases salary for some person on 100. Without &#064;IgniteAsyncCallback annotation
 *     // this operation is not safe.
 *     public boolean evaluate(CacheEntryEvent&lt;? extends K, ? extends V&gt; evt) throws CacheEntryListenerException {
 *         Person p = evt.getValue();
 *
 *         if (p.getSalary() &gt; 1000)
 *             return true;
 *
 *         ignite.cache("Person").put(evt.getKey(), new Person(p.getName(), p.getSalary() + 100));
 *
 *         return false;
 *     }
 * }
 * </pre>
 * <p>
 * Query with asynchronous callback execute as usually:
 * <pre name="code" class="java">
 * // Create new continuous query.
 * ContinuousQuery&lt;Long, Person&gt; qry = new ContinuousQuery&lt;&gt;();
 *
 * // Callback that is called locally when update notifications are received.
 * // It simply prints out information about all created persons.
 * qry.setLocalListener((evts) -> {
 *     for (CacheEntryEvent&lt;? extends Long, ? extends Person&gt; e : evts) {
 *         Person p = e.getValue();
 *
 *         System.out.println(p.getFirstName() + " " + p.getLastName() + "'s salary is " + p.getSalary());
 *     }
 * });
 *
 * // Sets remote filter.
 * qry.setRemoteFilterFactory(() -> new ExampleCacheEntryFilter());
 *
 * // Execute query.
 * QueryCursor&lt;Cache.Entry&lt;Long, Person&gt;&gt; cur = cache.query(qry);
 * </pre>
 *
 * @see IgniteConfiguration#getAsyncCallbackPoolSize
 * @see ContinuousQuery#getRemoteFilterFactory()
 * @see ContinuousQuery#getLocalListener()
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface IgniteAsyncCallback {
    // No-op.
}
