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

import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;

/**
 * Client listener protocol version.
 */
public class ClientListenerProtocolVersion implements Comparable<ClientListenerProtocolVersion> {
    /** Major part. */
    private final short major;

    /** Minor part. */
    private final short minor;

    /** Maintenance part. */
    private final short maintenance;

    /**
     * Create version.
     *
     * @param major Major part.
     * @param minor Minor part.
     * @param maintenance Maintenance part.
     * @return Version.
     */
    public static ClientListenerProtocolVersion create(int major, int minor, int maintenance) {
        return new ClientListenerProtocolVersion((short)major, (short)minor, (short)maintenance);
    }

    /**
     * Constructor.
     *
     * @param major Major part.
     * @param minor Minor part.
     * @param maintenance Maintenance part.
     */
    private ClientListenerProtocolVersion(short major, short minor, short maintenance) {
        this.major = major;
        this.minor = minor;
        this.maintenance = maintenance;
    }

    /**
     * @return Major part.
     */
    public short major() {
        return major;
    }

    /**
     * @return Minor part.
     */
    public short minor() {
        return minor;
    }

    /**
     * @return Maintenance part.
     */
    public short maintenance() {
        return maintenance;
    }

    /**
     * @return String representation of version.
     */
    public String asString() {
        return String.valueOf(major) + '.' + minor + '.' + maintenance;
    }

    /** {@inheritDoc} */
    @Override public int compareTo(@NotNull ClientListenerProtocolVersion other) {
        int res = major - other.major;

        if (res == 0)
            res = minor - other.minor;

        if (res == 0)
            res = maintenance - other.maintenance;

        return res;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return 31 * (31 * major + minor) + maintenance;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (obj != null && obj instanceof ClientListenerProtocolVersion) {
            ClientListenerProtocolVersion other = (ClientListenerProtocolVersion)obj;

            return F.eq(major, other.major) && F.eq(minor, other.minor) && F.eq(maintenance, other.maintenance);
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ClientListenerProtocolVersion.class, this);
    }
}
