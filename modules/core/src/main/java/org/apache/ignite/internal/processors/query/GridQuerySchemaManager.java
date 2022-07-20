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

package org.apache.ignite.internal.processors.query;

import java.util.List;
import java.util.function.BiConsumer;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Provides information about query engine schemas.
 */
public interface GridQuerySchemaManager {
    /**
     * Find type descriptor by schema and table name.
     *
     * @return Query type descriptor or {@code null} if descriptor was not found.
     */
    public @Nullable GridQueryTypeDescriptor typeDescriptorForTable(String schemaName, String tableName);

    /**
     * Find type descriptor by schema and index name.
     *
     * @return Query type descriptor or {@code null} if descriptor was not found.
     */
    public @Nullable GridQueryTypeDescriptor typeDescriptorForIndex(String schemaName, String idxName);

    /**
     * Find cache info by schema and table name.
     *
     * @return Cache info or {@code null} if cache info was not found.
     */
    public @Nullable <K, V> GridCacheContextInfo<K, V> cacheInfoForTable(String schemaName, String tableName);

    /**
     * Register listener for drop columns event.
     *
     * @param lsnr Drop columns event listener.
     */
    void registerDropColumnsListener(@NotNull BiConsumer<GridQueryTypeDescriptor, List<String>> lsnr);

    /**
     * Unregister listener for drop columns event.
     *
     * @param lsnr Drop columns event listener.
     */
    void unregisterDropColumnsListener(@NotNull BiConsumer<GridQueryTypeDescriptor, List<String>> lsnr);

    /**
     * Register listener for drop table event.
     *
     * @param lsnr Drop table event listener.
     */
    void registerDropTableListener(@NotNull BiConsumer<String, String> lsnr);

    /**
     * Unregister listener for drop table event.
     *
     * @param lsnr Drop table event listener.
     */
    void unregisterDropTableListener(@NotNull BiConsumer<String, String> lsnr);
}
