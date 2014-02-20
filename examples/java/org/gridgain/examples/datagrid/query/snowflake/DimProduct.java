// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.datagrid.query.snowflake;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;

/**
 * Represents a product available for purchase. In our {@code snowflake} schema a {@code product}
 * is a {@code 'dimension'} and will be cached in {@link GridCacheMode#REPLICATED}
 * cache.
 *
 * @author @java.author
 * @version @java.version
 */
public class DimProduct {
    /** Primary key. */
    @GridCacheQuerySqlField(unique = true)
    private int id;

    /** Product name. */
    private String name;

    /** Product list price. */
    @GridCacheQuerySqlField
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
        StringBuilder sb = new StringBuilder();

        sb.append("DimProduct ");
        sb.append("[id=").append(id);
        sb.append(", name=").append(name);
        sb.append(", price=").append(price);
        sb.append(", qty=").append(qty);
        sb.append(']');

        return sb.toString();
    }
}
