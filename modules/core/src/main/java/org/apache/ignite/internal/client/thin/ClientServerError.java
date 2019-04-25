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

package org.apache.ignite.internal.client.thin;

/**
 * Ignite server failed to process client request.
 */
public class ClientServerError extends ClientError {
    /** Server error code. */
    private final int code;

    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /**
     * Constructs a new exception with the specified detail message.
     */
    ClientServerError(String srvMsg, int srvCode, long reqId) {
        super(
            String.format("Ignite failed to process request [%s]: %s (server status code [%s])", reqId, srvMsg, srvCode)
        );

        code = srvCode;
    }

    /**
     * @return Server error code.
     */
    public int getCode() {
        return code;
    }
}
