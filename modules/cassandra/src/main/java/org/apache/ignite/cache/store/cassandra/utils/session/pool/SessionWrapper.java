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

package org.apache.ignite.cache.store.cassandra.utils.session.pool;

import com.datastax.driver.core.Session;
import org.apache.ignite.cache.store.cassandra.utils.common.CassandraHelper;

/**
 * Wrapper for Cassandra driver session, responsible for monitoring session expiration and its closing
 */
public class SessionWrapper {
    public static final long EXPIRATION_TIMEOUT = 300000;  //5 minutes

    private Session session;
    private long time;

    public SessionWrapper(Session session) {
        this.session = session;
        this.time = System.currentTimeMillis();
    }

    public boolean expired() {
        return System.currentTimeMillis() - time > EXPIRATION_TIMEOUT;
    }

    public Session driverSession() {
        return session;
    }

    public void release() {
        CassandraHelper.closeSession(session);
        session = null;
    }
}
