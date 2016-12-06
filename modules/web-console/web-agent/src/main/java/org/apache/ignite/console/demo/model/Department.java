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

package org.apache.ignite.console.demo.model;

import java.io.Serializable;

/**
 * Department definition.
 */
public class Department implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Value for id. */
    private int id;

    /** Value for countryId. */
    private int countryId;

    /** Value for name. */
    private String name;

    /**
     * Empty constructor.
     */
    public Department() {
        // No-op.
    }

    /**
     * Full constructor.
     */
    public Department(
        int id,
        int countryId,
        String name
    ) {
        this.id = id;
        this.countryId = countryId;
        this.name = name;
    }

    /**
     * Gets id.
     *
     * @return Value for id.
     */
    public int getId() {
        return id;
    }

    /**
     * Sets id.
     *
     * @param id New value for id.
     */
    public void setId(int id) {
        this.id = id;
    }

    /**
     * Gets countryId.
     *
     * @return Value for countryId.
     */
    public int getCountryId() {
        return countryId;
    }

    /**
     * Sets countryId.
     *
     * @param countryId New value for countryId.
     */
    public void setCountryId(int countryId) {
        this.countryId = countryId;
    }

    /**
     * Gets name.
     *
     * @return Value for name.
     */
    public String getName() {
        return name;
    }

    /**
     * Sets name.
     *
     * @param name New value for name.
     */
    public void setName(String name) {
        this.name = name;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        
        if (!(o instanceof Department))
            return false;

        Department that = (Department)o;

        if (id != that.id)
            return false;

        if (countryId != that.countryId)
            return false;

        if (name != null ? !name.equals(that.name) : that.name != null)
            return false;

        return true;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = id;

        res = 31 * res + countryId;

        res = 31 * res + (name != null ? name.hashCode() : 0);

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Department [id=" + id +
            ", countryId=" + countryId +
            ", name=" + name +
            ']';
    }
}
