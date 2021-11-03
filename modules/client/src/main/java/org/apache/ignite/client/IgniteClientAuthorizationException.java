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

package org.apache.ignite.client;

/**
 * Indicates that user is not authorized to perform an operation.
 */
public class IgniteClientAuthorizationException extends IgniteClientException {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Message. */
    private static final String MSG = "User is not authorized to perform this operation";

    /**
     * Default constructor.
     */
    public IgniteClientAuthorizationException() {
        super(MSG);
    }

    /**
     * Constructs a new exception with the specified cause and a detail message.
     *
     * @param cause the cause.
     */
    public IgniteClientAuthorizationException(Throwable cause) {
        super(MSG, cause);
    }

    /**
     * Constructs a new exception with the specified detail message and cause.
     *
     * @param msg   the detail message.
     * @param cause the cause.
     */
    public IgniteClientAuthorizationException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
