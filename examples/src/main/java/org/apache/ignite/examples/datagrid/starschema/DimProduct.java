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

package org.apache.ignite.examples.datagrid.starschema;

import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

/**
 * Represents a product available for purchase. In our {@code snowflake} schema a {@code product}
 * is a {@code 'dimension'} and will be cached in {@link CacheMode#REPLICATED}
 * cache.
 */
public class DimProduct {
    /** Primary key. */
    @QuerySqlField(index = true)
    private int id;

    /** Product name. */
    private String name;

    /** Product list price. */
    @QuerySqlField
    private float price;

    /** Available product quantity. */
    private int qty;

    /**
     * Constructs a product instance.
     *
     * @param id Product ID.
     * @param name Product name.
     * @param price Product list price.
     * @param qty Available product quantity.
     */
    public DimProduct(int id, String name, float price, int qty) {
        this.id = id;
        this.name = name;
        this.price = price;
        this.qty = qty;
    }

    /**
     * Gets product ID.
     *
     * @return Product ID.
     */
    public int getId() {
        return id;
    }

    /**
     * Gets product name.
     *
     * @return Product name.
     */
    public String getName() {
        return name;
    }

    /**
     * Gets product list price.
     *
     * @return Product list price.
     */
    public float getPrice() {
        return price;
    }

    /**
     * Gets available product quantity.
     *
     * @return Available product quantity.
     */
    public int getQuantity() {
        return qty;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "DimProduct [id=" + id +
            ", name=" + name +
            ", price=" + price +
            ", qty=" + qty + ']';
    }
}