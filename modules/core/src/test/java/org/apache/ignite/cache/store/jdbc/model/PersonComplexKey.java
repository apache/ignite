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
