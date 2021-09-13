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

import org.apache.ignite.internal.client.proto.ClientErrorCode;
import org.apache.ignite.lang.IgniteException;

/**
 * Common thin client unchecked exception.
 */
public class IgniteClientException extends IgniteException {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Error code. */
    private final int errorCode;

    /**
     * Constructs a new exception with {@code null} as its detail message.
     */
    public IgniteClientException() {
        errorCode = ClientErrorCode.FAILED;
    }

    /**
     * Constructs a new exception with the specified detail message.
     *
     * @param msg the detail message.
     */
    public IgniteClientException(String msg) {
        super(msg);

        this.errorCode = ClientErrorCode.FAILED;
    }

    /**
     * Constructs a new exception with the specified detail message.
     *
     * @param msg the detail message.
     * @param errorCode the error code.
     */
    public IgniteClientException(String msg, int errorCode) {
        super(msg);

        this.errorCode = errorCode;
    }

    /**
     * Constructs a new exception with the specified detail message and cause.
     *
     * @param msg the detail message.
     * @param errorCode the error code.
     * @param cause the cause.
     */
    public IgniteClientException(String msg, int errorCode, Throwable cause) {
        super(msg, cause);

        this.errorCode = errorCode;
    }

    /**
     * Constructs a new exception with the specified detail message and cause.
     *
     * @param msg the detail message.
     * @param cause the cause.
     */
    public IgniteClientException(String msg, Throwable cause) {
        super(msg, cause);

        this.errorCode = ClientErrorCode.FAILED;
    }

    /**
     * Gets the error code. See {@link ClientErrorCode}.
     *
     * @return Error code.
     */
    public int errorCode() {
        return errorCode;
    }
}
