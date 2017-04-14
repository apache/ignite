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

package org.apache.ignite.transactions;

/**
 * Transaction deadlock exception.
 * <p>
 * This exception can be thrown from any cache method that modifies or reads data within transaction
 * (explicit or implicit) with timeout in case when deadlock detection is enabled (enabled by default).
 * <p>
 * Usually this exception is cause for {@link TransactionTimeoutException}.
 */
public class TransactionDeadlockException extends TransactionException {
    /** Serial version UID. */
    private static final long serialVersionUID = 0L;

    /**
     * Creates new deadlock exception with given error message.
     *
     * @param msg Error message.
     */
    public TransactionDeadlockException(String msg) {
        super(msg);
    }
}
