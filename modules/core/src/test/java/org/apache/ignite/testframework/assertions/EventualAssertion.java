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

package org.apache.ignite.testframework.assertions;

/**
 * An {@link Assertion} that attempts to assert a condition within a maximum amount of time.
 */
public class EventualAssertion implements Assertion {
    /** The assertion condition. */
    private final Assertion assertion;

    /**
     * The amount of time in milliseconds to wait for the assertion to succeed. If zero try forever, if negative
     * try once and return immediately.
     */
    private final long timeout;

    /**
     * Create a new EventualAssertion.
     *
     * @param timeout The maximum amount of time in milliseconds to wait for the specified assertion to succeed; if zero
     * the implementation will try forever and if negative try once and return immediately.
     * @param assertion The assertion to test.
     */
    public EventualAssertion(long timeout, Assertion assertion) {
        if (assertion == null) {
            throw new IllegalArgumentException("Assertion cannot be null");
        }
        this.timeout = timeout;
        this.assertion = assertion;
    }

    /**
     * Test that the assertion is satisfied within the configured timeout.
     * <p>
     *     Note: this implementation uses an exponential back-off between successive retries of the assertion.
     *
     * @throws AssertionError If the condition was not satisfied within the timeout.
     */
    public void test() throws AssertionError {

        // compute the ending time
        long endTime = timeout == 0 ? Long.MAX_VALUE : System.currentTimeMillis() + this.timeout;

        // current wait time
        long waitTime = 2;

        // last assertion error
        Throwable lastError;
        do {
            try {
                this.assertion.test();
                return;
            }
            catch (final Throwable e) {
                lastError = e;
            }

            // see if we're done
            long currentTime = System.currentTimeMillis();
            if (currentTime > endTime) {
                break;
            }

            // wait for the current wait time
            try {
                synchronized (this) {
                    wait(waitTime);
                }
            }
            catch (InterruptedException e) {
                lastError = e;
                Thread.currentThread().interrupt();
                break;
            }

            // exponential back-off
            waitTime = Math.min(endTime - currentTime, Math.max(waitTime << 2, 8));
        }
        while (true);

        throw new AssertionError(this.assertion.getClass().getName() + " failed", lastError);
    }
}
