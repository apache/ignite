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
package org.apache.ignite.testframework;

/**
 * Allows to listen a log and run the callback when the message that matches the regexp is detected.
 */
public class CallbackExecutorLogListener extends LogListener {
    /** */
    private final String expectedMessage;

    /** */
    private final Runnable cb;

    /**
     * Default constructor.
     *
     * @param expectedMessage regexp for message that triggers the callback
     * @param cb callback
     */
    public CallbackExecutorLogListener(String expectedMessage, Runnable cb) {
        this.expectedMessage = expectedMessage;
        this.cb = cb;
    }

    /** {@inheritDoc} */
    @Override public boolean check() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        /* No-op */
    }

    /** {@inheritDoc} */
    @Override public void accept(String s) {
        if (s.matches(expectedMessage))
            cb.run();
    }
}
