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

package org.apache.ignite.table.mapper;

import java.util.function.Function;
import org.apache.ignite.table.Tuple;

/**
 * Record mapper interface.
 *
 * @param <R> Record type.
 */
public interface RecordMapper<R> {
    /**
     * Record mapper builder.
     *
     * @param <R> Record type.
     */
    public interface Builder<R> {
        /**
         * Map a field to a type of given class.
         *
         * @param fieldName Field name.
         * @param targetClass Target class.
         * @return {@code this} for chaining.
         */
        public Builder<R> map(String fieldName, Class<?> targetClass);

        /**
         * Adds a functional mapping for a field,
         * the result depends on function call for every particular row.
         *
         * @param fieldName Field name.
         * @param mappingFunction Mapper function.
         * @return {@code this} for chaining.
         */
        public Builder<R> map(String fieldName, Function<Tuple, Object> mappingFunction);

        /**
         * Builds record mapper.
         *
         * @return Mapper.
         */
        public RecordMapper<R> build();
    }
}
