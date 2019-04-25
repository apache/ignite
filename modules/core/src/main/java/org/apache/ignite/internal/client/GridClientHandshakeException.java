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
 * This exception is thrown when a client handshake has failed.
 */
public class GridClientHandshakeException extends GridClientException {
    /** */
    private static final long serialVersionUID = 0L;

    /** Status code for handshake error. */
    private final byte statusCode;

    /**
     * Constructor.
     *
     * @param statusCode Error status code.
     * @param msg Error message.
     */
    public GridClientHandshakeException(byte statusCode, String msg) {
        super(msg);

        this.statusCode = statusCode;
    }

    /**
     * @return Error status code.
     */
    public byte getStatusCode() {
        return statusCode;
    }
}