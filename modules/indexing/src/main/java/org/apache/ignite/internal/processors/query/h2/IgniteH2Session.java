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
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.h2.engine.Session;

/**
 * Wrapper for H2 session
 */
public class IgniteH2Session {
    /** */
    private final Session ses;

    /** */
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * Constructor.
     *
     * @param ses H2 session.
     */
    public IgniteH2Session(Session ses) {
        this.ses = ses;
    }

    /**
     *
     */
    public void lockTables() {
        // Prevent reentrant tables read-lock because H2 lock semantic: one table unlock must release all locks.
        try {
            if (!lock.isHeldByCurrentThread()) {
                lock.lockInterruptibly();

                GridH2Table.readLockTables(ses);
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedException("Thread got interrupted while trying to acquire table lock.", e);
        }
    }

    /**
     *
     */
    public void unlockTables() {
        GridH2Table.unlockTables(ses);

        if (lock.isHeldByCurrentThread())
            lock.unlock();
    }

    /**
     *
     */
    public void checkTablesVersions() {
        GridH2Table.checkTablesVersions(ses);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteH2Session.class, this);
    }
}
