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

package org.apache.ignite.internal.processors.rest.client.message;

import java.io.Serializable;
import org.apache.ignite.internal.client.GridClientCacheMode;

/**
 * Cache bean.
 */
public class GridClientCacheBean implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Cache name
     */
    private String name;

    /**
     * Cache mode
     */
    private GridClientCacheMode mode;

    /**
     * Custom name of the sql schema.
     */
    private String sqlSchema;

    public GridClientCacheBean() {
    }

    public GridClientCacheBean(String name, GridClientCacheMode mode, String sqlSchema) {
        this.name = name;
        this.mode = mode;
        this.sqlSchema = sqlSchema;
    }

    /**
     * Gets cache name.
     *
     * @return Cache name.
     */
    public String getName() {
        return name;
    }

    /**
     * Sets cache name.
     *
     * @param name Cache name.
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Gets cache mode.
     *
     * @return Cache mode.
     */
    public GridClientCacheMode getMode() {
        return mode;
    }

    /**
     * Sets cache mode.
     *
     * @param mode Cache mode.
     */
    public void setMode(GridClientCacheMode mode) {
        this.mode = mode;
    }

    /**
     * Gets custom name of the sql schema.
     *
     * @return Custom name of the sql schema.
     */
    public String getSqlSchema() {
        return sqlSchema;
    }

    /**
     * Sets custom name of the sql schema.
     *
     * @param sqlSchema Custom name of the sql schema.
     */
    public void setSqlSchema(String sqlSchema) {
        this.sqlSchema = sqlSchema;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (obj == null || getClass() != obj.getClass())
            return false;

        GridClientCacheBean other = (GridClientCacheBean) obj;

        return name == null ? other.name == null : name.equals(other.name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "GridClientCacheBean [name=" + name + ", mode=" + mode + ']';
    }
}
