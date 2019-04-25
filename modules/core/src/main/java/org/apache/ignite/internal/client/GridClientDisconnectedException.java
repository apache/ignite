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
 * This exception is thrown when client has no Grid topology and (probably temporary) can't obtain it.
 */
public class GridClientDisconnectedException extends GridClientException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates exception with given message and cause.
     *
     * @param msg Error message.
     * @param cause Cause exception.
     */
    public GridClientDisconnectedException(String msg, GridClientException cause) {
        super(msg, cause);
    }
}