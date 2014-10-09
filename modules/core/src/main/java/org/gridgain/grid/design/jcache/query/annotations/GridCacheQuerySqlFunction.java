/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.design.jcache.query.annotations;

import java.lang.annotation.*;

/**
 * Annotates public static methods in classes to be used in SQL queries as custom functions.
 * Annotated class must be registered in H2 indexing SPI using following method
 * {@gglink org.gridgain.grid.spi.indexing.h2.GridH2IndexingSpi#setIndexCustomFunctionClasses(java.lang.Class[])}.
 * <p>
 * Example usage:
 * <pre name="code" class="java">
 *     public class MyFunctions {
 *         &#64;GridCacheQuerySqlFunction
 *         public static int sqr(int x) {
 *             return x * x;
 *         }
 *     }
 *
 *     // Register.
 *     indexing.setIndexCustomFunctionClasses(MyFunctions.class);
 *
 *     // And use in queries.
 *     cache.queries().createSqlFieldsQuery("select sqr(2) where sqr(1) = 1");
 * </pre>
 * <p>
 * For more information about H2 custom functions please refer to
 * <a href="http://www.h2database.com/html/features.html#user_defined_functions">H2 documentation</a>.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface GridCacheQuerySqlFunction {
    /**
     * Specifies alias for the function to be used form SQL queries.
     * If no alias provided method name will be used.
     *
     * @return Alias for function.
     */
    String alias() default "";

    /**
     * Specifies if the function is deterministic (result depends only on input parameters).
     * <p>
     * Deterministic function is a function which always returns the same result
     * assuming that input parameters are the same.
     *
     * @return {@code true} If function is deterministic, {@code false} otherwise.
     */
    boolean deterministic() default false;
}
