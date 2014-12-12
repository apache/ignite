/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.query;

import java.io.*;
import java.util.*;

/**
 * Query configuration object.
 */
public class GridCacheQueryConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Collection of query type metadata. */
    private Collection<GridCacheQueryTypeMetadata> typeMeta;

    /** Query type resolver. */
    private GridCacheQueryTypeResolver typeRslvr;

    /** */
    private boolean idxPrimitiveKey;

    /** */
    private boolean idxPrimitiveVal;

    /** */
    private boolean idxFixedTyping;

    /** */
    private boolean escapeAll;

    /**
     * Default constructor.
     */
    public GridCacheQueryConfiguration() {
        // No-op.
    }

    /**
     * @param cfg Configuration to copy.
     */
    public GridCacheQueryConfiguration(GridCacheQueryConfiguration cfg) {
        typeMeta = cfg.getTypeMetadata();
        typeRslvr = cfg.getTypeResolver();
        idxPrimitiveKey = cfg.isIndexPrimitiveKey();
        idxPrimitiveVal = cfg.isIndexPrimitiveValue();
        idxFixedTyping = cfg.isIndexFixedTyping();
        escapeAll = cfg.isEscapeAll();
    }

    /**
     * Gets collection of query type metadata objects.
     *
     * @return Collection of query type metadata.
     */
    public Collection<GridCacheQueryTypeMetadata> getTypeMetadata() {
        return typeMeta;
    }

    /**
     * Sets collection of query type metadata objects.
     *
     * @param typeMeta Collection of query type metadata.
     */
    public void setTypeMetadata(Collection<GridCacheQueryTypeMetadata> typeMeta) {
        this.typeMeta = typeMeta;
    }

    /**
     * Gets query type resolver.
     *
     * @return Query type resolver.
     */
    public GridCacheQueryTypeResolver getTypeResolver() {
        return typeRslvr;
    }

    /**
     * Sets query type resolver.
     *
     * @param typeRslvr Query type resolver.
     */
    public void setTypeResolver(GridCacheQueryTypeResolver typeRslvr) {
        this.typeRslvr = typeRslvr;
    }

    /**
     * Gets flag indicating whether SQL engine should index by key in cases
     * where key is primitive type
     *
     * @return {@code True} if primitive keys should be indexed.
     */
    public boolean isIndexPrimitiveKey() {
        return idxPrimitiveKey;
    }

    /**
     * Sets flag indicating whether SQL engine should index by key in cases
     * where key is primitive type.
     *
     * @param idxPrimitiveKey {@code True} if primitive keys should be indexed.
     */
    public void setIndexPrimitiveKey(boolean idxPrimitiveKey) {
        this.idxPrimitiveKey = idxPrimitiveKey;
    }

    /**
     * Gets flag indicating whether SQL engine should index by value in cases
     * where value is primitive type
     *
     * @return {@code True} if primitive values should be indexed.
     */
    public boolean isIndexPrimitiveValue() {
        return idxPrimitiveVal;
    }

    /**
     * Sets flag indexing whether SQL engine should index by value in cases
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
     * If {@code false}, SQL engine will store all keys in BINARY form to make it possible to store
     * the same value type with different key types. If {@code true}, key type will be converted
     * to respective SQL type if it is possible, hence, improving performance of queries.
     * <p>
     * Setting this value to {@code false} also means that {@code '_key'} column cannot be indexed and
     * cannot participate in query where clauses. The behavior of using '_key' column in where
     * clauses with this flag set to {@code false} is undefined.
     *
     * @return {@code True} if SQL engine should try to convert values to their respective SQL
     *      types for better performance.
     */
    public boolean isIndexFixedTyping() {
        return idxFixedTyping;
    }

    /**
     * This flag essentially controls whether key type is going to be identical
     * for all values of the same type.
     * <p>
     * If false, SQL engine will store all keys in BINARY form to make it possible to store
     * the same value type with different key types. If true, key type will be converted
     * to respective SQL type if it is possible, which may provide significant performance
     * boost.
     *
     * @param idxFixedTyping {@code True} if SQL engine should try to convert values to their respective SQL
     *      types for better performance.
     */
    public void setIndexFixedTyping(boolean idxFixedTyping) {
        this.idxFixedTyping = idxFixedTyping;
    }

    /**
     * If {@code true}, then table name and all column names in 'create table' SQL
     * generated for SQL engine are escaped with double quotes. This flag should be set if table name of
     * column name is H2 reserved word or is not valid H2 identifier (e.g. contains space or hyphen).
     * <p>
     * Note if this flag is set then table and column name in SQL queries also must be escaped with double quotes.

     * @return Flag value.
     */
    public boolean isEscapeAll() {
        return escapeAll;
    }

    /**
     * If {@code true}, then table name and all column names in 'create table' SQL
     * generated for SQL engine are escaped with double quotes. This flag should be set if table name of
     * column name is H2 reserved word or is not valid H2 identifier (e.g. contains space or hyphen).
     * <p>
     * Note if this flag is set then table and column name in SQL queries also must be escaped with double quotes.

     * @param escapeAll Flag value.
     */
    public void setEscapeAll(boolean escapeAll) {
        this.escapeAll = escapeAll;
    }
}
