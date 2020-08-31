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

package org.apache.ignite.internal.jdbc.thin;

import java.util.EnumSet;
import java.util.UUID;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcThinFeature;
import org.apache.ignite.lang.IgniteProductVersion;

/**
 * Handshake result.
 */
class HandshakeResult {
    /** Ignite server version. */
    private IgniteProductVersion igniteVer;

    /** Node Id. */
    private UUID nodeId;

    /** Current protocol version used to connection to Ignite. */
    private ClientListenerProtocolVersion srvProtoVer;

    /** Features. */
    private EnumSet<JdbcThinFeature> features = EnumSet.noneOf(JdbcThinFeature.class);

    /**
     * @return Ignite server version.
     */
    IgniteProductVersion igniteVersion() {
        return igniteVer;
    }

    /**
     * @param igniteVer New ignite server version.
     */
    void igniteVersion(IgniteProductVersion igniteVer) {
        this.igniteVer = igniteVer;
    }

    /**
     * @return Node Id.
     */
    UUID nodeId() {
        return nodeId;
    }

    /**
     * @param nodeId New node Id.
     */
    void nodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * @return Current protocol version used to connection to Ignite.
     */
    ClientListenerProtocolVersion serverProtocolVersion() {
        return srvProtoVer;
    }

    /**
     * @param srvProtoVer New current protocol version used to connection to Ignite.
     */
    void serverProtocolVersion(ClientListenerProtocolVersion srvProtoVer) {
        this.srvProtoVer = srvProtoVer;
    }

    /**
     * @param features Supported features.
     */
    public void features(EnumSet<JdbcThinFeature> features) {
        this.features = features;
    }

    /**
     * @return Supported features.
     */
    public EnumSet<JdbcThinFeature> features() {
        return features;
    }
}
