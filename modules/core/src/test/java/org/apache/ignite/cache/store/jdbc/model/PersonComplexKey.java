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

package org.apache.ignite.cache.store.jdbc.model;

import java.io.Serializable;

/**
 * PersonComplexKey definition.
 */
public class PersonComplexKey implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Value for id. */
    private int id;

    /** Value for org id. */
    private int orgId;

    /** Value for city id. */
    private int cityId;

    /**
     * Empty constructor.
     */
    public PersonComplexKey() {
        // No-op.
    }

    /**
     * Full constructor.
     */
    public PersonComplexKey(
        int id,
        int orgId,
        int cityId
    ) {
        this.id = id;
        this.orgId = orgId;
        this.cityId = cityId;
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
     * Gets orgId.
     *
     * @return Value for orgId.
     */
    public int getOrgId() {
        return orgId;
    }

    /**
     * Sets orgId.
     *
     * @param orgId New value for orgId.
     */
    public void setOrgId(int orgId) {
        this.orgId = orgId;
    }

    /**
     * Gets cityId.
     */
    public int getCityId() {
        return cityId;
    }

    /**
     * Sets city id.
     *
     * @param cityId New value for cityId.
     */
    public void setCityId(int cityId) {
        this.cityId = cityId;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof PersonComplexKey))
            return false;

        PersonComplexKey that = (PersonComplexKey)o;

        if (id != that.id || orgId != that.orgId || cityId != that.cityId)
            return false;

        return true;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = id;

        res = 31 * res + orgId;
        res = 31 * res + cityId;

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "PersonKey [id=" + id +
            ", orgId=" + orgId +
            ", cityId=" + cityId +
            "]";
    }
}
