/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.examples.model;

import java.sql.Timestamp;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

/**
 * This class represents organization object.
 */
public class Organization {
    /** */
    private static final AtomicLong ID_GEN = new AtomicLong();

    /** Organization ID (indexed). */
    @QuerySqlField(index = true)
    private Long id;

    /** Organization name (indexed). */
    @QuerySqlField(index = true)
    private String name;

    /** Address. */
    private Address addr;

    /** Type. */
    private OrganizationType type;

    /** Last update time. */
    private Timestamp lastUpdated;

    /**
     * Required for binary deserialization.
     */
    public Organization() {
        // No-op.
    }

    /**
     * @param name Organization name.
     */
    public Organization(String name) {
        id = ID_GEN.incrementAndGet();

        this.name = name;
    }

    /**
     * @param id Organization ID.
     * @param name Organization name.
     */
    public Organization(long id, String name) {
        this.id = id;
        this.name = name;
    }

    /**
     * @param name Name.
     * @param addr Address.
     * @param type Type.
     * @param lastUpdated Last update time.
     */
    public Organization(String name, Address addr, OrganizationType type, Timestamp lastUpdated) {
        id = ID_GEN.incrementAndGet();

        this.name = name;
        this.addr = addr;
        this.type = type;

        this.lastUpdated = lastUpdated;
    }

    /**
     * @return Organization ID.
     */
    public Long id() {
        return id;
    }

    /**
     * @return Name.
     */
    public String name() {
        return name;
    }

    /**
     * @return Address.
     */
    public Address address() {
        return addr;
    }

    /**
     * @return Type.
     */
    public OrganizationType type() {
        return type;
    }

    /**
     * @return Last update time.
     */
    public Timestamp lastUpdated() {
        return lastUpdated;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Organization [id=" + id +
            ", name=" + name +
            ", address=" + addr +
            ", type=" + type +
            ", lastUpdated=" + lastUpdated + ']';
    }
}
