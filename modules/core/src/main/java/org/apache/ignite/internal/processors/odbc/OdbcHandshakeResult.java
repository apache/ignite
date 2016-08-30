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
 * ODBC handshake result.
 */
public class OdbcHandshakeResult {
    /** Handshake accepted. */
    private final boolean accepted;

    /** Apache Ignite version when protocol version has been introduced. */
    private final String protoVerSince;

    /** Current Apache Ignite version. */
    private final String curVer;

    /**
     * Constructor.
     *
     * @param accepted Indicates whether handshake accepted or not.
     * @param protoVerSince Apache Ignite version when protocol version has been introduced.
     * @param curVer Current Apache Ignite version.
     */
    public OdbcHandshakeResult(boolean accepted, String protoVerSince, String curVer) {
        this.accepted = accepted;
        this.protoVerSince = protoVerSince;
        this.curVer = curVer;
    }

    /**
     * @return Query ID.
     */
    public boolean accepted() {
        return accepted;
    }

    /**
     * @return Apache Ignite version when protocol version has been introduced.
     */
    public String protocolVersionSince() {
        return protoVerSince;
    }

    /**
     * @return Current Apache Ignite version.
     */
    public String currentVersion() {
        return curVer;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(OdbcHandshakeResult.class, this);
    }
}
