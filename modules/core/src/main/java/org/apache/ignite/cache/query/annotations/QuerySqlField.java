/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.cache.query.annotations;

import java.lang.annotation.*;

/**
 * Annotates fields for SQL queries. All fields that will be involved in SQL clauses must have
 * this annotation. For more information about cache queries see {@link org.gridgain.grid.cache.query.GridCacheQuery} documentation.
 * @see org.gridgain.grid.cache.query.GridCacheQuery
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.FIELD})
public @interface QuerySqlField {
    /**
     * Specifies whether cache should maintain an index for this field or not.
     * Just like with databases, field indexing may require additional overhead
     * during updates, but makes select operations faster.
     * <p>
     * When {@gglink org.gridgain.grid.spi.indexing.h2.GridH2IndexingSpi} is set as indexing SPI and indexed field is
     * of type {@code com.vividsolutions.jts.geom.Geometry} (or any subclass of this class) then GridGain will
     * consider this index as spatial providing performance boost for spatial queries.
     *
     * @return {@code True} if index must be created for this field in database.
     */
    boolean index() default false;

    /**
     * Specifies whether index should be unique or not. This property only
     * makes sense if {@link #index()} property is set to {@code true}.
     *
     * @return {@code True} if field index should be unique.
     * @deprecated No longer supported, will be ignored.
     */
    @Deprecated
    boolean unique() default false;

    /**
     * Specifies whether index should be in descending order or not. This property only
     * makes sense if {@link #index()} property is set to {@code true}.
     *
     * @return {@code True} if field index should be in descending order.
     */
    boolean descending() default false;

    /**
     * Array of index groups this field belongs to. Groups are used for compound indexes,
     * whenever index should be created on more than one field. All fields within the same
     * group will belong to the same index.
     * <p>
     * Group indexes are needed because SQL engine can utilize only one index per table occurrence in a query.
     * For example if we have two separate indexes on fields {@code a} and {@code b} of type {@code X} then
     * query {@code select * from X where a = ? and b = ?} will use for filtering either index on field {@code a}
     * or {@code b} but not both. For more effective query execution here it is preferable to have a single
     * group index on both fields.
     * <p>
     * For more complex scenarios please refer to {@link QuerySqlField.Group} documentation.
     *
     * @return Array of group names.
     */
    String[] groups() default {};

    /**
     * Array of ordered index groups this field belongs to. For more information please refer to
     * {@linkplain QuerySqlField.Group} documentation.
     *
     * @return Array of ordered group indexes.
     * @see #groups()
     */
    Group[] orderedGroups() default {};

    /**
     * Property name. If not provided then field name will be used.
     *
     * @return Name of property.
     */
    String name() default "";

    /**
     * Describes group of index and position of field in this group.
     * <p>
     * Opposite to {@link #groups()} this annotation gives control over order of fields in a group index.
     * This can be needed in scenarios when we have a query like
     * {@code select * from X where a = ? and b = ? order by b desc}. If we have index {@code (a asc, b asc)}
     * sorting on {@code b} will be performed. Here it is preferable to have index {@code (b desc, a asc)}
     * which will still allow query to search on index using both fields and avoid sorting because index
     * is already sorted in needed way.
     *
     * @see #groups()
     * @see #orderedGroups()
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.METHOD, ElementType.FIELD})
    @SuppressWarnings("PublicInnerClass")
    public static @interface Group {
        /**
         * Group index name where this field participate.
         *
         * @return Group index name
         */
        String name();

        /**
         * Fields in this group index will be sorted on this attribute.
         *
         * @return Order number.
         */
        int order();

        /**
         * Defines sorting order for this field in group.
         *
         * @return True if field will be in descending order.
         */
        boolean descending() default false;
    }
}
