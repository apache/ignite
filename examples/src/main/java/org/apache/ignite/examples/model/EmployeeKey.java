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

package org.apache.ignite.examples.model;

import org.apache.ignite.cache.affinity.AffinityKeyMapped;

/**
 * This class represents key for employee object.
 * <p>
 * Used in query example to collocate employees
 * with their organizations.
 */
public class EmployeeKey {
    /** ID. */
    private int id;

    /** Organization ID. */
    @AffinityKeyMapped
    private int organizationId;

    /**
     * Required for binary deserialization.
     */
    public EmployeeKey() {
        // No-op.
    }

    /**
     * @param id ID.
     * @param organizationId Organization ID.
     */
    public EmployeeKey(int id, int organizationId) {
        this.id = id;
        this.organizationId = organizationId;
    }

    /**
     * @return ID.
     */
    public int id() {
        return id;
    }

    /**
     * @return Organization ID.
     */
    public int organizationId() {
        return organizationId;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        EmployeeKey key = (EmployeeKey)o;

        return id == key.id && organizationId == key.organizationId;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = id;

        res = 31 * res + organizationId;

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "EmployeeKey [id=" + id +
            ", organizationId=" + organizationId + ']';
    }
}
