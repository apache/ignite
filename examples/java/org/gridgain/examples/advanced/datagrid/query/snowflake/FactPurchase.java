// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.advanced.datagrid.query.snowflake;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;

/**
 * Represents a purchase record. In our {@code snowflake} schema purchase
 * is a {@code 'fact'} and will be cached in larger {@link GridCacheMode#PARTITIONED}
 * cache.
 *
 * @author @java.author
 * @version @java.version
 */
public class FactPurchase {
    /** Primary key. */
    @GridCacheQuerySqlField(unique = true)
    private int id;

    /** Foreign key to store at which purchase occurred. */
    @GridCacheQuerySqlField
    private int storeId;

    /** Foreign key to purchased product. */
    @GridCacheQuerySqlField
    private int productId;

    /** Purchase price. */
    @GridCacheQuerySqlField
    private float purchasePrice;

    /**
     * Constructs a purchase record.
     *
     * @param id Purchase ID.
     * @param productId Purchased product ID.
     * @param storeId Store ID.
     * @param purchasePrice Purchase price.
     */
    public FactPurchase(int id, int productId, int storeId, float purchasePrice) {
        this.id = id;
        this.productId = productId;
        this.storeId = storeId;
        this.purchasePrice = purchasePrice;
    }

    /**
     * Gets purchase ID.
     *
     * @return Purchase ID.
     */
    public int getId() {
        return id;
    }

    /**
     * Gets purchased product ID.
     *
     * @return Product ID.
     */
    public int getProductId() {
        return productId;
    }

    /**
     * Gets ID of store at which purchase was made.
     *
     * @return Store ID.
     */
    public int getStoreId() {
        return storeId;
    }

    /**
     * Gets purchase price.
     *
     * @return Purchase price.
     */
    public float getPurchasePrice() {
        return purchasePrice;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("FactPurchase ");
        sb.append("[id=").append(id);
        sb.append(", productId=").append(productId);
        sb.append(", storeId=").append(storeId);
        sb.append(", purchasePrice=").append(purchasePrice);
        sb.append(']');

        return sb.toString();
    }
}
