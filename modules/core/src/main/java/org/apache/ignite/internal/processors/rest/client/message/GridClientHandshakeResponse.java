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

package org.apache.ignite.internal.processors.rest.client.message;

import java.io.Externalizable;

/**
 * A client handshake response, containing result
 * code.
 */
public class GridClientHandshakeResponse extends GridClientAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final byte CODE_OK = 0;

    /** Response, indicating successful handshake. */
    public static final GridClientHandshakeResponse OK = new GridClientHandshakeResponse(CODE_OK);

    /** */
    private byte resCode;

    /**
     * Constructor for {@link Externalizable}.
     */
    public GridClientHandshakeResponse() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param resCode Result code.
     */
    public GridClientHandshakeResponse(byte resCode) {
        this.resCode = resCode;
    }

    /**
     * @return Result code.
     */
    public byte resultCode() {
        return resCode;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return getClass().getSimpleName() + " [resCode=" + resCode + ']';
    }
}