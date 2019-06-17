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

package org.apache.ignite.console.web.model;

import org.apache.ignite.internal.util.typedef.internal.S;
import org.springframework.http.HttpStatus;

/**
 * Error response JSON bean with the error code and technical withError message.
 */
public class ErrorResponse {
    /** */
    private int code;

    /** */
    private String msg;

    /**
     * Default constructor for serialization.
     */
    public ErrorResponse() {
        // No-op.
    }

    /**
     * Full constructor.
     *
     * @param code Error code.
     * @param msg Error message.
     */
    public ErrorResponse(int code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    /**
     * Full constructor.
     *
     * @param status HTTP status.
     * @param msg Error message.
     */
    public ErrorResponse(HttpStatus status, String msg) {
        this(status.value(), msg);
    }

    /**
     * @return Error code.
     */
    public int getCode() {
        return code;
    }

    /**
     * @param code Error code.
     */
    public void setCode(int code) {
        this.code = code;
    }

    /**
     * @return Error message.
     */
    public String getMessage() {
        return msg;
    }

    /**
     * @param msg Error message.
     */
    public void setMessage(String msg) {
        this.msg = msg;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ErrorResponse.class, this);
    }
}
