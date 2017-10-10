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
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.CacheConfiguration;

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
     * Index inline size in bytes. When enabled part of indexed value will be placed directly to index pages,
     * thus minimizing data page accesses, thus increasing query performance.
     * <p>
     * Allowed values:
     * <ul>
     *     <li>{@code -1} (default) - determine inline size automatically (see below)</li>
     *     <li>{@code 0} - index inline is disabled (not recommended)</li>
     *     <li>positive value - fixed index inline</li>
     * </ul>
     * When set to {@code -1}, Ignite will try to detect inline size automatically. It will be no more than
     * {@link CacheConfiguration#getSqlIndexMaxInlineSize()}. Index inline will be enabled for all fixed-length types,
     * but <b>will not be enabled</b> for {@code String}.
     *
     * @return Index inline size in bytes.
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