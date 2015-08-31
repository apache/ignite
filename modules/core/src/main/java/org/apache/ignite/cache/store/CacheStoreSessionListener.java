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

package org.apache.ignite.cache.store;

import javax.cache.configuration.Factory;
import javax.sql.DataSource;
import org.apache.ignite.cache.store.jdbc.CacheJdbcStoreSessionListener;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

/**
 * Cache store session listener that allows to implement callbacks
 * for session lifecycle.
 * <p>
 * The most common use case for session listeners is database
 * connection and transaction management. Store can be invoked one
 * or several times during one session, depending on whether it's
 * executed within cache transaction or not. In any case, you have
 * to create a connection when session is started and commit it or
 * rollback when session is finished.
 * <p>
 * Cache store session listener allows to implement this and other
 * scenarios providing to callback methods:
 * <ul>
 *     <li>
 *         {@link #onSessionStart(CacheStoreSession)} - called
 *         before any store operation within a session is invoked.
 *     </li>
 *     <li>
 *         {@link #onSessionEnd(CacheStoreSession, boolean)} - called
 *         after all operations within a session are invoked.
 *     </li>
 * </ul>
 * <h2>Implementations</h2>
 * Ignites provides several out-of-the-box implementations
 * of session listener (refer to individual JavaDocs for more
 * details):
 * <ul>
 *     <li>
 *         {@link CacheJdbcStoreSessionListener} - JDBC-based session
 *         listener. For each session it gets a new JDBC connection from
 *         provided {@link DataSource} and commits (or rolls back) it
 *         when session ends.
 *     </li>
 *     <li>
 *         {@ignitelink org.apache.ignite.cache.store.spring.CacheSpringStoreSessionListener} -
 *         session listener based on Spring transaction management.
 *         It starts a new DB transaction for each session and commits
 *         (or rolls back) it when session ends. If there is no ongoing
 *         cache transaction, this listener is no-op.
 *     </li>
 *     <li>
 *         {@ignitelink org.apache.ignite.cache.store.hibernate.CacheHibernateStoreSessionListener} -
 *         Hibernate-based session listener. It creates a new Hibernate
 *         session for each Ignite session. If there is an ongoing cache
 *         transaction, a corresponding Hibernate transaction is created
 *         as well.
 *     </li>
 * </ul>
 * <h2>Configuration</h2>
 * There are two ways to configure a session listener:
 * <ul>
 *     <li>
 *         Provide a global listener for all caches via
 *         {@link IgniteConfiguration#setCacheStoreSessionListenerFactories(Factory[])}
 *         configuration property. This will we called for any store
 *         session, not depending on what caches participate in
 *         transaction.
 *     </li>
 *     <li>
 *         Provide a listener for a particular cache via
 *         {@link CacheConfiguration#setCacheStoreSessionListenerFactories(Factory[])}
 *         configuration property. This will be called only if the
 *         cache participates in transaction.
 *     </li>
 * </ul>
 * For example, here is how global {@link CacheJdbcStoreSessionListener}
 * can be configured in Spring XML configuration file:
 * <pre name="code" class="xml">
 * &lt;bean class="org.apache.ignite.configuration.IgniteConfiguration"&gt;
 *     ...
 *
 *     &lt;property name="CacheStoreSessionListenerFactories"&gt;
 *         &lt;list&gt;
 *             &lt;bean class="javax.cache.configuration.FactoryBuilder$SingletonFactory"&gt;
 *                 &lt;constructor-arg&gt;
 *                     &lt;bean class="org.apache.ignite.cache.store.jdbc.CacheJdbcStoreSessionListener"&gt;
 *                         &lt;!-- Inject external data source. --&gt;
 *                         &lt;property name="dataSource" ref="jdbc-data-source"/&gt;
 *                     &lt;/bean&gt;
 *                 &lt;/constructor-arg&gt;
 *             &lt;/bean&gt;
 *         &lt;/list&gt;
 *     &lt;/property&gt;
 * &lt;/bean&gt;
 * </pre>
 */
public interface CacheStoreSessionListener {
    /**
     * On session start callback.
     * <p>
     * Called before any store operation within a session is invoked.
     *
     * @param ses Current session.
     */
    public void onSessionStart(CacheStoreSession ses);

    /**
     * On session end callback.
     * <p>
     * Called after all operations within a session are invoked.
     *
     * @param ses Current session.
     * @param commit {@code True} if persistence store transaction
     *      should commit, {@code false} for rollback.
     */
    public void onSessionEnd(CacheStoreSession ses, boolean commit);
}