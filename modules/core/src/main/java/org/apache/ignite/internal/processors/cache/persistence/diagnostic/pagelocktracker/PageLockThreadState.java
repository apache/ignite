/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker;

import java.util.Objects;

/**
 *
 */
class PageLockThreadState {
    /** */
    final long threadOpCnt;

    /** */
    final long heldLockCnt;

    /** */
    final Thread thread;

    /** */
    PageLockThreadState(long threadOpCnt, long heldLockCnt, Thread thread) {
        this.threadOpCnt = threadOpCnt;
        this.heldLockCnt = heldLockCnt;
        this.thread = thread;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        PageLockThreadState state = (PageLockThreadState)o;
        return threadOpCnt == state.threadOpCnt &&
            heldLockCnt == state.heldLockCnt &&
            Objects.equals(thread, state.thread);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(threadOpCnt, heldLockCnt, thread);
    }
}
