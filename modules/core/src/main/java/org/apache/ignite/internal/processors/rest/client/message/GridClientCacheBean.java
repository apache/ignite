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
    @Override public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override public boolean equals(Object obj) {
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
    @Override public String toString() {
        return "GridClientCacheBean [name=" + name + ", mode=" + mode + ']';
    }
}
