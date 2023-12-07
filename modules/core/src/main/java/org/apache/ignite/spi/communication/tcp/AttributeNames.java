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

package org.apache.ignite.spi.communication.tcp;

import org.apache.ignite.cluster.ClusterNode;

/**
 * This class was created for the refactoring approach. It contains attribute names into a {@link ClusterNode}.
 * It should be remove after global refactoring. The values of these attributes should pass via constructors of appropriate classes.
 * @deprecated fix it in the ticket https://ggsystems.atlassian.net/browse/GG-29546
 */
@Deprecated
public class AttributeNames {
    /** Paired connection. */
    private final String pairedConn;

    /** Addresses. */
    private final String addrs;

    /** Host names. */
    private final String hostNames;

    /** Externalizable attributes. */
    private final String extAttrs;

    /** Port. */
    private final String port;

    /** */
    private final String forceClientServerConnections;

    /**
     * @param pairedConn Paired connection.
     * @param addrs Addresses.
     * @param hostNames Host names.
     * @param extAttrs Externalizable attributes.
     * @param port Port.
     * @param forceClientServerConnections Force client server connections.
     */
    public AttributeNames(
        String pairedConn,
        String addrs,
        String hostNames,
        String extAttrs,
        String port,
        String forceClientServerConnections) {
        this.pairedConn = pairedConn;
        this.addrs = addrs;
        this.hostNames = hostNames;
        this.extAttrs = extAttrs;
        this.port = port;
        this.forceClientServerConnections = forceClientServerConnections;
    }

    /**
     * @return Paired connection.
     */
    public String pairedConnection() {
        return pairedConn;
    }

    /**
     * @return Externalizable attributes.
     */
    public String externalizableAttributes() {
        return extAttrs;
    }

    /**
     * @return Host names.
     */
    public String hostNames() {
        return hostNames;
    }

    /**
     * @return Addresses.
     */
    public String addresses() {
        return addrs;
    }

    /**
     * @return Port.
     */
    public String port() {
        return port;
    }

    /**
     * @return Force client server connections.
     */
    public String getForceClientServerConnections() {
        return forceClientServerConnections;
    }
}
