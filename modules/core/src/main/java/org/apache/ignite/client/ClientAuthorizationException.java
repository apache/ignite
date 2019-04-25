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
 * Indicates user has no permission to perform operation.
 */
public class ClientAuthorizationException extends ClientException {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Message. */
    private static final String MSG = "User is not authorized to perform this operation";

    /**
     * Default constructor.
     */
    public ClientAuthorizationException() {
        super(MSG);
    }

    /**
     * Constructs a new exception with the specified cause and a detail
     * message of <tt>(cause==null ? null : cause.toString())</tt>.
     *
     * @param cause the cause.
     */
    public ClientAuthorizationException(Throwable cause) {
        super(MSG, cause);
    }
}
