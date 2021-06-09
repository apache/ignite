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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.raft.jraft.Closure;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.util.Requires;

/**
 *
 */
public class JoinableClosure implements Closure {

    private final CountDownLatch latch = new CountDownLatch(1);
    private final Closure closure;

    public JoinableClosure(Closure closure) {
        this.closure = Requires.requireNonNull(closure, "closure");
    }

    @Override
    public void run(final Status status) {
        this.closure.run(status);
        latch.countDown();
    }

    public void join() throws InterruptedException {
        this.latch.await();
    }

    public void join(final long timeoutMillis) throws InterruptedException, TimeoutException {
        if (!this.latch.await(timeoutMillis, TimeUnit.MILLISECONDS)) {
            throw new TimeoutException("joined timeout");
        }
    }

    public Closure getClosure() {
        return closure;
    }
}
