/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.closure;

import java.util.concurrent.CountDownLatch;
import org.apache.ignite.raft.jraft.Closure;
import org.apache.ignite.raft.jraft.Status;

/**
 * A special Closure which provides synchronization primitives.
 */
public class SynchronizedClosure implements Closure {

    private CountDownLatch latch;
    private volatile Status status;
    /**
     * Latch count to reset
     */
    private int count;

    public SynchronizedClosure() {
        this(1);
    }

    public SynchronizedClosure(final int n) {
        this.count = n;
        this.latch = new CountDownLatch(n);
    }

    /**
     * Get last ran status
     *
     * @return returns the last ran status
     */
    public Status getStatus() {
        return this.status;
    }

    @Override
    public void run(final Status status) {
        this.status = status;
        this.latch.countDown();
    }

    /**
     * Wait for closure run
     *
     * @return status
     * @throws InterruptedException if the current thread is interrupted while waiting
     */
    public Status await() throws InterruptedException {
        this.latch.await();
        return this.status;
    }

    /**
     * Reset the closure
     */
    public void reset() {
        this.status = null;
        this.latch = new CountDownLatch(this.count);
    }
}
