/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.examples.datagrid.starschema;

import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

/**
 * Represents a physical store location. In our {@code snowflake} schema a {@code store}
 * is a {@code 'dimension'} and will be cached in {@link CacheMode#REPLICATED}
 * cache.
 */
public class DimStore {
    /** Primary key. */
    @QuerySqlField(index = true)
    private int id;

    /** Store name. */
    @QuerySqlField
    private String name;

    /** Zip code. */
    private String zip;

    /** Address. */
    private String addr;

    /**
     * Constructs a store instance.
     *
     * @param id Store ID.
     * @param name Store name.
     * @param zip Store zip code.
     * @param addr Store address.
     */
    public DimStore(int id, String name, String zip, String addr) {
        this.id = id;
        this.name = name;
        this.zip = zip;
        this.addr = addr;
    }

    /**
     * Gets store ID.
     *
     * @return Store ID.
     */
    public int getId() {
        return id;
    }

    /**
     * Gets store name.
     *
     * @return Store name.
     */
    public String getName() {
        return name;
    }

    /**
     * Gets store zip code.
     *
     * @return Store zip code.
     */
    public String getZip() {
        return zip;
    }

    /**
     * Gets store address.
     *
     * @return Store address.
     */
    public String getAddress() {
        return addr;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "DimStore [id=" + id +
            ", name=" + name +
            ", zip=" + zip +
            ", addr=" + addr + ']';
    }
}