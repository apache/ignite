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

package org.apache.ignite.internal.client;

/**
 * Client future timeout exception is thrown whenever any client waiting is timed out.
 */
public class GridClientFutureTimeoutException extends GridClientException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates exception with specified error message.
     *
     * @param msg Error message.
     */
    public GridClientFutureTimeoutException(String msg) {
        super(msg);
    }

    /**
     * Creates exception with specified error message and cause.
     *
     * @param msg Error message.
     * @param cause Error cause.
     */
    public GridClientFutureTimeoutException(String msg, Throwable cause) {
        super(msg, cause);
    }
}