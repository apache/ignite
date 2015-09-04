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

package org.apache.ignite.internal.processors.cache.query;

import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;

/**
 * Extended query cursor interface allowing for "getAll" to output data into destination other than Collection.
 */
public interface QueryCursorEx<T> extends QueryCursor<T> {
    /**
     * Get all values passing them through passed consumer.
     *
     * @param c Consumer.
     */
    public void getAll(Consumer<T> c) throws IgniteCheckedException;

    /**
     * @return Query metadata.
     */
    public List<GridQueryFieldMetadata> fieldsMeta();

    /**
     * Query value consumer.
     */
    public static interface Consumer<T> {
        /**
         * Consume value.
         *
         * @param val Value.
         * @throws IgniteCheckedException If failed.
         */
        public void consume(T val) throws IgniteCheckedException;
    }
}