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

package org.apache.ignite.internal.processors.odbc;

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * ODBC handshake request.
 */
public class OdbcHandshakeRequest extends OdbcRequest {
    /** Protocol version. */
    private final OdbcProtocolVersion ver;

    /** Distributed joins flag. */
    private boolean distributedJoins = false;

    /** Enforce join order flag. */
    private boolean enforceJoinOrder = false;

    /**
     * @param ver Long value for protocol version.
     */
    public OdbcHandshakeRequest(long ver) {
        super(HANDSHAKE);

        this.ver = OdbcProtocolVersion.fromLong(ver);
    }

    /**
     * @return Protocol version.
     */
    public OdbcProtocolVersion version() {
        return ver;
    }

    /**
     * @return Distributed joins flag.
     */
    public boolean distributedJoins() {
        return distributedJoins;
    }

    /**
     * @param distributedJoins Distributed joins flag.
     */
    public void distributedJoins(boolean distributedJoins) {
        this.distributedJoins = distributedJoins;
    }

    /**
     * @return Enforce join order flag.
     */
    public boolean enforceJoinOrder() {
        return enforceJoinOrder;
    }

    /**
     * @param enforceJoinOrder Enforce join order flag.
     */
    public void enforceJoinOrder(boolean enforceJoinOrder) {
        this.enforceJoinOrder = enforceJoinOrder;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(OdbcHandshakeRequest.class, this);
    }
}
