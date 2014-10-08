/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.design.queries.annotations;

import java.lang.annotation.*;

/**
 * Describes group index.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface GridCacheQueryGroupIndex {
    /**
     * Group index name.
     *
     * @return Name.
     */
    String name();

    /**
     * If this index is unique.
     *
     * @return True if this index is unique, false otherwise.
     * @deprecated No longer supported, will be ignored.
     */
    @Deprecated
    boolean unique() default false;

    /**
     * List of group indexes for type.
     */
    @SuppressWarnings("PublicInnerClass")
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public static @interface List {
        /**
         * Gets array of group indexes.
         *
         * @return Array of group indexes.
         */
        GridCacheQueryGroupIndex[] value();
    }
}
