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
package org.apache.ignite.internal.processors.cache.persistence.checkpoint;

import org.apache.ignite.internal.util.future.CountDownFuture;
import org.jetbrains.annotations.Nullable;

/**
 * Count down future. In addition to superclass this implementation allows to increment waiting tasks count.
 */
class CountDownDynamicFuture extends CountDownFuture {
    /**
     * Flag protecting from submitting new tasks when future was already completed
     */
    private volatile boolean completionObserved;

    /**
     * @param cnt Number of completing parties.
     */
    CountDownDynamicFuture(int cnt) {
        super(cnt);
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable Void res, @Nullable Throwable err) {
        final boolean done = super.onDone(res, err);

        completionObserved |= done;

        return done;
    }

    /**
     * Grows counter of submitted tasks to be waited for complete.
     *
     * Call this method only if counter can't become 0.
     */
    void incrementTasksCount() {
        if (completionObserved)
            throw new IllegalStateException("Future already completed, not allowed to submit new tasks");

        remaining().incrementAndGet();
    }
}
