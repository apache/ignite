/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.agent.dto.action;

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * DTO for response error.
 */
public class ResponseError {
    /** Internal error code. */
    public static final int INTERNAL_ERROR_CODE = -32603;

    /** Authentication error code. */
    public static final int AUTHENTICATION_ERROR_CODE = -32001;

    /** Authorize error code. */
    public static final int AUTHORIZE_ERROR_CODE = -32002;

    /** Parse error code. */
    public static final int PARSE_ERROR_CODE = -32700;

    /** Code. */
    private int code;

    /** Message. */
    private String msg;

    /** Stack trace. */
    private StackTraceElement[] stackTrace;

    /**
     * Default constructor.
     */
    public ResponseError() {
        // No-op
    }

    /**
     * @param code Code.
     * @param msg Message.
     * @param stackTrace Stack trace.
     */
    public ResponseError(int code, String msg, StackTraceElement[] stackTrace) {
        this.code = code;
        this.msg = msg;
        this.stackTrace = stackTrace;
    }

    /**
     * @return Error code.
     */
    public int getCode() {
        return code;
    }

    /**
     * @param code Code.
     * @return {@code This} for chaining method calls.
     */
    public ResponseError setCode(int code) {
        this.code = code;

        return this;
    }

    /**
     * @return Error message.
     */
    public String getMessage() {
        return msg;
    }

    /**
     * @param msg Message.
     * @return {@code This} for chaining method calls.
     */
    public ResponseError setMessage(String msg) {
        this.msg = msg;

        return this;
    }

    /**
     * @return Stack trace.
     */
    public StackTraceElement[] getStackTrace() {
        return stackTrace;
    }

    /**
     * @param stackTrace Stack trace.
     * @return {@code This} for chaining method calls.
     */
    public ResponseError setStackTrace(StackTraceElement[] stackTrace) {
        this.stackTrace = stackTrace;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ResponseError.class, this);
    }
}
