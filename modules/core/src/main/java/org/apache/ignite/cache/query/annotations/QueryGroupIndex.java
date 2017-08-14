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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import javax.cache.CacheException;
import org.apache.ignite.cache.QueryIndex;

/**
 * Describes group index.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface QueryGroupIndex {
    /**
     * Group index name.
     *
     * @return Name.
     */
    String name();

    /**
     * Index inline size.
     *
     * The optimization is used on index creation. The part of the indexed fields is placed (inlined) directly into
     * index page to avoid excessive data page reads when using index.
     *
     * Inline size value must be greater or equal than zero or {@link QueryIndex#DFLT_INLINE_SIZE} (default).
     * The {@link CacheException} is thrown when inlineSize is invalid.
     *
     * For composite index all filed are concatenated and the first {@code inlineSize} bytes is used to inline.
     *
     * Avoid to specify {@link QuerySqlField#inlineSize()} for composite index. The {@link CacheException}
     * is thrown on the processing such types.
     *
     * @return The size in bytes of the index inline.
     */
    int inlineSize() default QueryIndex.DFLT_INLINE_SIZE;

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
        QueryGroupIndex[] value();
    }
}