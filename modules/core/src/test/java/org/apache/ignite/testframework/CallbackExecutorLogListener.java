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

import org.apache.ignite.lang.IgniteInClosure;

/**
 * Allows to listen a log and run the callback when the message that matches the regexp is detected.
 */
public class CallbackExecutorLogListener extends LogListener {
    /** Regexp for message that triggers callback */
    private final String expMsg;

    /** Callback. */
    private final IgniteInClosure<String> cb;

    /**
     * Constructor.
     *
     * @param expMsg Regexp for message that triggers callback.
     * @param cb Callback.
     */
    public CallbackExecutorLogListener(String expMsg, Runnable cb) {
        this(expMsg, s -> cb.run());
    }

    /**
     * Constructor.
     *
     * @param expMsg Regexp for message that triggers callback.
     * @param cb Callback.
     */
    public CallbackExecutorLogListener(String expMsg, IgniteInClosure<String> cb) {
        this.expMsg = expMsg;
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
        if (s.matches(expMsg))
            cb.apply(s);
    }
}
