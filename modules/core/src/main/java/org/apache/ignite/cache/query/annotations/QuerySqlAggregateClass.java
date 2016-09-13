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

import java.lang.annotation.*;

/**
 * Annotates public classes to be used in SQL queries as custom aggregate functions.
 * Annotated class must be registered in H2 indexing SPI using following method
 * {@link org.apache.ignite.configuration.CacheConfiguration#setSqlAggregateClasses(Class[])}.
 * <p>
 * Example usage:
 * <pre name="code" class="java">
 *    @QuerySqlAggregateFunction
 *    public static class SimpleAggregate implements org.h2.api.AggregateFunction {
 *        private List list = new ArrayList();
 *
 *        @Override
 *        public void init(Connection conn) throws SQLException {
 *
 *        }
 *
 *        @Override
 *        public int getType(int[] inputTypes) throws SQLException {
 *          return Types.JAVA_OBJECT;
 *        }
 *
 *        @Override
 *        public void add(Object value) throws SQLException {
 *          list.add(value);
 *        }
 *
 *        @Override
 *        public Object getResult() throws SQLException {
 *          return list;
 *        }
 *    }
 *
 *     // Register.
 *     indexing.setSqlAggregateClasses(MyFunctions.class);
 *
 *     // And use in queries.
 *     cache.queries().createSqlFieldsQuery("select int_array(2, 3, 5) ");
 * </pre>
 * <p>
 * For more information about H2 custom functions please refer to
 * <a href="http://www.h2database.com/html/grammar.html#create_aggregate">H2 documentation</a>.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface QuerySqlAggregateClass {
    /**
     * Specifies alias for the function to be used form SQL queries.
     * If no alias provided method name will be used.
     *
     * @return Alias for function.
     */
    String alias() default "";


}