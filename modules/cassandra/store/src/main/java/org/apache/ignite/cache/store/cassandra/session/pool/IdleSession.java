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

package org.apache.ignite.cache.store.cassandra.session.pool;

import com.datastax.driver.core.Session;
import org.apache.ignite.cache.store.cassandra.common.CassandraHelper;

/**
 * Simple wrapper for idle Cassandra session returned to pool, responsible for monitoring session expiration and its closing.
 */
public class IdleSession {
    /** Cassandra driver session. */
    private Session ses;

    /** Expiration timeout. */
    private long expirationTimeout;

    /** Wrapper creation time.  */
    private long time;

    /**
     * Creates instance of Cassandra driver session wrapper.
     *
     * @param ses Cassandra driver session.
     */
    public IdleSession(Session ses, long expirationTimeout) {
        this.ses = ses;
        this.expirationTimeout = expirationTimeout;
        this.time = System.currentTimeMillis();
    }

    /**
     * Checks if Cassandra driver session expired.
     *
     * @return true if session expired.
     */
    public boolean expired() {
        return expirationTimeout > 0 && System.currentTimeMillis() - time > expirationTimeout;
    }

    /**
     * Returns wrapped Cassandra driver session.
     *
     * @return Cassandra driver session.
     */
    public Session driverSession() {
        return ses;
    }

    /**
     * Closes wrapped Cassandra driver session
     */
    public void release() {
        CassandraHelper.closeSession(ses);
        ses = null;
    }
}
