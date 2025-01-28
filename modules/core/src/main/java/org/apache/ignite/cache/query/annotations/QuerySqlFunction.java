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

package org.apache.ignite.cache.query.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.apache.ignite.resources.SessionContextProviderResource;

/**
 * Annotates public methods in classes to be used in SQL queries as custom functions.
 * Annotated class must be registered using following method
 * {@link org.apache.ignite.configuration.CacheConfiguration#setSqlFunctionClasses(Class[])}.
 * <p>
 * Example usage:
 * <pre name="code" class="java">
 *     public class MyFunctions {
 *         &#64;QuerySqlFunction
 *         public static int sqr(int x) {
 *             return x * x;
 *         }
 *     }
 *
 *     // Register in CacheConfiguration.
 *     cacheCfg.setSqlFunctionClasses(MyFunctions.class);
 *
 *     // And use in queries.
 *     cache.query(new SqlFieldsQuery("select sqr(2) where sqr(1) = 1"));
 * </pre>
 * <p>
 * SQL function can be a table function. Result of a table function is treated as a row set (a table) and can be used with
 * other SQL operators.
 * <p>
 * Example usage:
 * <pre name="code" class="java">
 *     public class MyTableFunctions {
 *         &#64;QuerySqlFunction(tableColumnTypes = {Float.class, String.class}, tableColumnNames = {&#34;F_VAL&#34;, &#34;S_VAL&#34;})
 *         public static Iterable&#60;Object[]&#62; my_table(int i, Float f, String str) {
 *             return Arrays.asList(
 *                 new Object[]{f, &#34;code_&#34; + (i * 10) + &#34;: &#34; + str},
 *                 new Object[]{null, &#34;code_&#34; + (i * 20) + &#34;: &#34; + str},
 *                 new Object[]{f,  &#34;code_&#34; + (i * 30) + &#34;: &#34; + str}
 *             );
 *         }
 *     }
 *
 *     // Register in CacheConfiguration.
 *     cacheCfg.setSqlFunctionClasses(MyTableFunctions.class);
 *
 *     // And use in queries.
 *     cache.query(new SqlFieldsQuery("select S_VAL from MY_TABLE(1, 5.0f, "ext") where F_VAL is not null"));
 * </pre>
 * <p>
 * To make the function a table function, define at least the table column types. Optionally, the column names can be also set.
 * <p>
 * A table function must return an {@code Iterable} as a row set. Each row can be represented by an {@code Object[]} or
 * by an {@code Iterable} too. Row length must match the defined number of column types. Row value types must match the
 * defined column types or can be assigned to them.
 * <p>
 * SQL functions can use attributes set on client side:
 * <pre name="code" class="java">
 *     public class MyFunctions {
 *         &#64;SessionContextProviderResource
 *         public SessionContextProvider sesCtxProv;
 *
 *         &#64;QuerySqlFunction
 *         public String sessionId() {
 *             return sesCtxProv.getSessionContext().getAttribute("SESSION_ID");
 *         }
 *     }
 * </pre>
 * <p>
 * Note, accessing to the attributes is available in the Calcite query engine only. In a such case a class must have public
 * zero-args constructor. The table functions also are available currently only with Calcite.
 *
 * @see SessionContextProviderResource
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface QuerySqlFunction {
    /**
     * Specifies alias for the function to be used form SQL queries.
     * If no alias provided method name will be used.
     *
     * @return Alias for function.
     */
    String alias() default "";

    /**
     * Makes the function a table function and defines column types of the returned SQL table representation.
     *
     * @return Column types of the table if this function is a table function. By default, is empty meaning the function
     * is not a table function.
     */
    Class<?>[] tableColumnTypes() default {};

    /**
     * Makes the function a table function and defines column names of the returned SQL table representation. Number
     * of the names must match number of the column types {@link #tableColumnTypes()} or be empty. If are empty while
     * {@link #tableColumnTypes()} is set, the default column names are used ('COL_1', 'COL_2', etc.).
     *
     * @return Table column names if this function is a table function. Is empty to use the default names or if
     * the function is not a table function.
     * @see #tableColumnTypes()
     */
    String[] tableColumnNames() default {};

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
