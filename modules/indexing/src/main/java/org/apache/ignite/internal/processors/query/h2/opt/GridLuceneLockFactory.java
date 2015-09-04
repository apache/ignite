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
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;

/**
 * Lucene {@link LockFactory} implementation.
 */
public class GridLuceneLockFactory extends LockFactory {
    /** */
    @SuppressWarnings("TypeMayBeWeakened")
    private final GridConcurrentHashSet<String> locks = new GridConcurrentHashSet<>();

    /** {@inheritDoc} */
    @Override public Lock makeLock(String lockName) {
        return new LockImpl(lockName);
    }

    /** {@inheritDoc} */
    @Override public void clearLock(String lockName) throws IOException {
        locks.remove(lockName);
    }

    /**
     * {@link Lock} Implementation.
     */
    private class LockImpl extends Lock {
        /** */
        private final String lockName;

        /**
         * @param lockName Lock name.
         */
        private LockImpl(String lockName) {
            this.lockName = lockName;
        }

        /** {@inheritDoc} */
        @Override public boolean obtain() throws IOException {
            return locks.add(lockName);
        }

        /** {@inheritDoc} */
        @Override public void release() throws IOException {
            locks.remove(lockName);
        }

        /** {@inheritDoc} */
        @Override public boolean isLocked() throws IOException {
            return locks.contains(lockName);
        }
    }
}