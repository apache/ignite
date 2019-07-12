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

package org.apache.ignite.client;

/**
 * Indicates all the Ignite servers specified in the client configuration are no longer available.
 */
public class ClientConnectionException extends ClientException {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /**
     * Constructs a new exception with the specified detail message.
     *
     * @param msg the detail message.
     */
    public ClientConnectionException(String msg) {
        super(msg);
    }

    /**
     * Constructs a new exception with the specified cause and detail message.
     *
     * @param msg the detail message.
     * @param cause the cause.
     */
    public ClientConnectionException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
