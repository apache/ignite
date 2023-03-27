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

package org.apache.ignite.internal.processors.query.h2.opt;

import java.io.IOException;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.LockObtainFailedException;

/**
 * Lucene {@link LockFactory} implementation.
 */
public class GridLuceneLockFactory extends LockFactory {
    /** */
    private final GridConcurrentHashSet<String> locks = new GridConcurrentHashSet<>();

    /** {@inheritDoc} */
    @Override public Lock obtainLock(Directory dir, String lockName) throws IOException {
        if (locks.add(lockName))
            return new LockImpl(lockName);
        else
            throw new LockObtainFailedException("lock instance already obtained: (dir=" + dir + ", lockName=" + lockName + ")");
    }

    /**
     * {@link Lock} Implementation.
     */
    private class LockImpl extends Lock {
        /** */
        private final String lockName;

        /** */
        private volatile boolean closed;

        /**
         * @param lockName Lock name.
         */
        private LockImpl(String lockName) {
            this.lockName = lockName;
        }

        /** {@inheritDoc} */
        @Override public void ensureValid() throws IOException {
            if (closed)
                throw new AlreadyClosedException("Lock instance already released: " + this);

            // check we are still in the locks map (some debugger or something crazy didn't remove us)
            if (!locks.contains(lockName))
                throw new AlreadyClosedException("Lock instance was invalidated from map: " + this);
        }

        /** {@inheritDoc} */
        @Override public void close() throws IOException {
            if (closed)
                return;

            try {
                if (!locks.remove(lockName))
                    throw new AlreadyClosedException("Lock was already released: " + this);
            }
            finally {
                closed = true;
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return super.toString() + ": " + lockName;
        }
    }
}
