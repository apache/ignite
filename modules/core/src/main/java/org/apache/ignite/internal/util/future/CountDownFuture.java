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

package org.apache.ignite.internal.util.future;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class CountDownFuture extends GridFutureAdapter<Void> {
    /** */
    private AtomicInteger remaining;

    /** */
    private AtomicReference<Exception> errCollector;

    /**
     * @param cnt Number of completing parties.
     */
    public CountDownFuture(int cnt) {
        remaining = new AtomicInteger(cnt);
        errCollector = new AtomicReference<>();
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable Void res, @Nullable Throwable err) {
        if (err != null)
            addError(err);

        int left = remaining.decrementAndGet();

        boolean done = left == 0 && super.onDone(res, errCollector.get());

        if (done)
            afterDone();

        return done;
    }

    /**
     *
     */
    protected void afterDone() {
        // No-op, to be overridden in subclasses.
    }

    /**
     * @param err Error.
     */
    private void addError(Throwable err) {
        Exception ex = errCollector.get();

        if (ex == null) {
            Exception compound = new IgniteCheckedException("Compound exception for CountDownFuture.");

            ex = errCollector.compareAndSet(null, compound) ? compound : errCollector.get();
        }

        assert ex != null;

        ex.addSuppressed(err);
    }
}
