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
package org.apache.ignite.internal.util;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Allows to execute callback when a set of operations will be completed. Also has an execution counter,
 * which allows to block callback execution in case if it is below given threshold. By default, threshold
 * is 0 and nothing blocks callback execution.
 *
 * <p>
 * <b>Sample usage:</b>:
 * <pre>{@code
 *     GridCountDownCallback countDownCb = new GridCountDownCallback(
 *         n,                     //internal counter is initiated with n
 *         () -> doSomething()    //callback
 *     );
 *
 *     //each call of countDown() decrements internal counter
 *     //doSomething() will be executed after counter reaches 0
 *     for (int i = 0; i < n; i++)
 *         new Thread(() -> countDownCb.countDown()).start();
 * }</pre>
 *
 * <p>
 * <b>Usage with execution threshold:</b>:
 * <pre>{@code
 *     GridCountDownCallback countDownCb = new GridCountDownCallback(
 *         n,                     //internal counter is initiated with n
 *         () -> doSomething(),   //callback
 *         n/2                    //execution threshold is initiated with n/2
 *     );
 *
 *     //a half of calls of countDown() increase execution counter, so it reaches threshold and callback executes.
 *     //doSomething() will be executed after n threads will perform countDown()
 *     for (int i = 0; i < n; i++)
 *         new Thread(() -> countDownCb.countDown(n % 2 == 0)).start();
 * }</pre>
 */
public class GridCountDownCallback {
    /** */
    private final AtomicInteger cntr;

    /** */
    private final int executionThreshold;

    /** */
    private final AtomicInteger executionCntr = new AtomicInteger(0);

    /** */
    private final Runnable cb;

    /**
     * Constructor.
     *
     * @param initCnt count of invocations of {@link #countDown}.
     * @param cb callback which will be executed after <code>initialCount</code>
     * invocations of {@link #countDown}.
     * @param executionThreshold minimal count of really performed operations to execute callback.
     */
    public GridCountDownCallback(int initCnt, Runnable cb, int executionThreshold) {
        cntr = new AtomicInteger(initCnt);

        this.executionThreshold = executionThreshold;

        this.cb = cb;
    }

    /**
     * Constructor. Execution threshold is set to 0.
     *
     * @param initCnt count of invocations of {@link #countDown}.
     * @param cb callback which will be executed after <code>initialCount</code>
     * invocations of {@link #countDown}.
     */
    public GridCountDownCallback(int initCnt, Runnable cb) {
        this(initCnt, cb, 0);
    }

    /**
     * Decrements the internal counter. If counter becomes 0, callback will be executed.
     *
     * @param doIncreaseExecutionCounter whether to increase execution counter
     */
    public void countDown(boolean doIncreaseExecutionCounter) {
        if (doIncreaseExecutionCounter)
            executionCntr.incrementAndGet();

        if (cntr.decrementAndGet() == 0 && executionCntr.get() >= executionThreshold)
            cb.run();
    }

    /**
     * Decrements the internal counter. If counter becomes 0, callback will be executed.
     */
    public void countDown() {
        countDown(true);
    }
}
