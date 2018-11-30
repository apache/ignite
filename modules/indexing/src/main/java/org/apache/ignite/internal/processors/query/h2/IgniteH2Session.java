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

package org.apache.ignite.internal.processors.query.h2;

import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.h2.engine.Session;

/**
 * Wrapper for H2 session
 */
public class IgniteH2Session {
    /** */
    private Session ses;

    /** */
    private ReentrantLock lock = new ReentrantLock();

    /**
     * Constructor.
     *
     * @param ses H2 session.
     */
    public IgniteH2Session(Session ses) {
        this.ses = ses;

        // The first lock of the tables is happened inside H2 SQL executor.
        // We have lock the session before obtain table lock to prevent
        // concurrent close the ResultSet.
        lock.lock();
    }

    /**
     *
     */
    public void lockTables() {
        // Prevent reentrant tables read-lock because H2 lock semantic: one table unlock must release all locks.
        if (!lock.isHeldByCurrentThread()) {
            lock.lock();

            if (ses != null)
                GridH2Table.readLockTables(ses);
        }
    }

    /**
     *
     */
    public void unlockTables() {
        if (lock.isHeldByCurrentThread()) {
            if (ses != null)
                GridH2Table.unlockTables(ses);

            lock.unlock();
        }
    }

    /**
     *
     */
    public void checkTablesVersions() {
        if (ses != null)
            GridH2Table.checkTablesVersions(ses);
    }

    /**
     *
     */
    public void release() {
        ses = null;
    }
}
