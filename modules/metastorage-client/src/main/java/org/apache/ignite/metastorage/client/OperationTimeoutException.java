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

package org.apache.ignite.metastorage.client;

/**
 * Thrown when an operation is not executed within a specified time period. Usually in such cases the operation
 * should be retried.
 */
public class OperationTimeoutException extends RuntimeException {
    /**
     * Constructs an exception.
     */
    public OperationTimeoutException() {
        super();
    }

    /**
     * Constructs an exception with a given message.
     *
     * @param message Detail message.
     */
    public OperationTimeoutException(String message) {
        super(message);
    }

    /**
     * Constructs an exception with a given message and a cause.
     *
     * @param message Detail message.
     * @param cause Cause.
     */
    public OperationTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs an exception with a given cause.
     *
     * @param cause Cause.
     */
    public OperationTimeoutException(Throwable cause) {
        super(cause);
    }
}
