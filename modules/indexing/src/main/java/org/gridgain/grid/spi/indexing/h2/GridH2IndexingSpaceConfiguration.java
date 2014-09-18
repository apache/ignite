/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.indexing.h2;

import org.gridgain.grid.util.typedef.internal.*;

/**
 * Defines per-space configuration properties for {@link GridH2IndexingSpi}.
 */
public class GridH2IndexingSpaceConfiguration {
    /** */
    private String name;

    /** */
    private boolean idxPrimitiveKey;

    /** */
    private boolean idxPrimitiveVal;

    /** */
    private boolean idxFixedTyping;

    /**
     * Gets space name to which this configuration applies.
     *
     * @return Space name.
     */
    public String getName() {
        return name;
    }

    /**
     * Sets space name.
     *
     * @param name Space name.
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Gets flag indicating whether indexing SPI should index by key in cases
     * where key is primitive type
     *
     * @return {@code True} if primitive keys should be indexed.
     */
    public boolean isIndexPrimitiveKey() {
        return idxPrimitiveKey;
    }

    /**
     * Sets flag indicating whether indexing SPI should index by key in cases
     * where key is primitive type.
     *
     * @param idxPrimitiveKey {@code True} if primitive keys should be indexed.
     */
    public void setIndexPrimitiveKey(boolean idxPrimitiveKey) {
        this.idxPrimitiveKey = idxPrimitiveKey;
    }

    /**
     * Gets flag indicating whether indexing SPI should index by value in cases
     * where value is primitive type
     *
     * @return {@code True} if primitive values should be indexed.
     */
    public boolean isIndexPrimitiveValue() {
        return idxPrimitiveVal;
    }

    /**
     * Sets flag indexing whether indexing SPI should index by value in cases
     * where value is primitive type.
     *
     * @param idxPrimitiveVal {@code True} if primitive values should be indexed.
     */
    public void setIndexPrimitiveValue(boolean idxPrimitiveVal) {
        this.idxPrimitiveVal = idxPrimitiveVal;
    }

    /**
     * This flag essentially controls whether all values of the same type have
     * identical key type.
     * <p>
     * If {@code false}, SPI will store all keys in BINARY form to make it possible to store
     * the same value type with different key types. If {@code true}, key type will be converted
     * to respective SQL type if it is possible, hence, improving performance of queries.
     * <p>
     * Setting this value to {@code false} also means that {@code '_key'} column cannot be indexed and
     * cannot participate in query where clauses. The behavior of using '_key' column in where
     * clauses with this flag set to {@code false} is undefined.
     *
     * @return {@code True} if SPI should try to convert values to their respective SQL
     *      types for better performance.
     */
    public boolean isIndexFixedTyping() {
        return idxFixedTyping;
    }

    /**
     * This flag essentially controls whether key type is going to be identical
     * for all values of the same type.
     * <p>
     * If false, SPI will store all keys in BINARY form to make it possible to store
     * the same value type with different key types. If true, key type will be converted
     * to respective SQL type if it is possible, which may provide significant performance
     * boost.
     *
     * @param idxFixedTyping {@code True} if SPI should try to convert values to their respective SQL
     *      types for better performance.
     */
    public void setIndexFixedTyping(boolean idxFixedTyping) {
        this.idxFixedTyping = idxFixedTyping;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridH2IndexingSpaceConfiguration.class, this);
    }
}
