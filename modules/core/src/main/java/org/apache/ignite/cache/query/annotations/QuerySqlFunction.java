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
 * Note, accessing to the attributes is available in the Calcite query engine only. In a such case a class must have public
 * zero-args constructor.
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
     * Specifies if the function is deterministic (result depends only on input parameters).
     * <p>
     * Deterministic function is a function which always returns the same result
     * assuming that input parameters are the same.
     *
     * @return {@code true} If function is deterministic, {@code false} otherwise.
     */
    boolean deterministic() default false;
}
