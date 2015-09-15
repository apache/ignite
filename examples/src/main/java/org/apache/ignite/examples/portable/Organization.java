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

package org.apache.ignite.examples.portable;

import java.sql.Timestamp;

/**
 * This class represents organization object.
 */
public class Organization {
    /** Name. */
    private String name;

    /** Address. */
    private Address address;

    /** Type. */
    private OrganizationType type;

    /** Last update time. */
    private Timestamp lastUpdated;

    /**
     * Required for portable deserialization.
     */
    public Organization() {
        // No-op.
    }

    /**
     * @param name Name.
     * @param address Address.
     * @param type Type.
     * @param lastUpdated Last update time.
     */
    public Organization(String name, Address address, OrganizationType type, Timestamp lastUpdated) {
        this.name = name;
        this.address = address;
        this.type = type;
        this.lastUpdated = lastUpdated;
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
        return address;
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
        return "Organization [name=" + name +
            ", address=" + address +
            ", type=" + type +
            ", lastUpdated=" + lastUpdated + ']';
    }
}
