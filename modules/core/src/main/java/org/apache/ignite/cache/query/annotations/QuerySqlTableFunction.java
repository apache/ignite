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
 * Annotates public methods in classes to be used in SQL queries as custom table functions. Annotated class must be
 * registered using {@link org.apache.ignite.configuration.CacheConfiguration#setSqlFunctionClasses(Class[])}.
 * <p>
 * Usage example:
 * <pre name="code" class="java">
 *     public class MyTableFunctions {
 *         &#64;QuerySqlTableFunction(columnTypes = {Float.class, String.class}, columnNames = {&#34;F_VAL&#34;, &#34;S_VAL&#34;})
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
 * Table function must return an {@code Iterable} as a row set. Each row can be represented by an {@code Object[]} or
 * by an {@code Collection}. Row length must match the defined number of column types. Row value types must match the
 * defined column types or be able assigned to them.
 * <p>
 * Note, the table functions are available currently only with Calcite.
 *
 * @see QuerySqlFunction
 * @see SessionContextProviderResource
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface QuerySqlTableFunction {
    /**
     * Specifies alias for the table function name to be used form SQL queries. If no alias provided, the method name is used.
     *
     * @return Alias for table function name.
     */
    String alias() default "";

    /**
     * Defines column types of the returned SQL table representation. Number of the types must match number of {@link #columnNames()}.
     *
     * @return Table column types.
     */
    Class<?>[] columnTypes();

    /**
     * Defines column names of the returned SQL table representation. Number of the names must match number of {@link #columnTypes()}.
     *
     * @return Table column names.
     */
    String[] columnNames();
}
