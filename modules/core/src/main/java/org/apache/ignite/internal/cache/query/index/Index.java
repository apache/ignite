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

package org.apache.ignite.internal.cache.query.index;

import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.jetbrains.annotations.Nullable;

/**
 * Basic interface for Ignite indexes.
 */
public interface Index {
    /**
     * Unique ID.
     */
    public UUID id();

    /**
     * Index name.
     */
    public String name();

    /**
     * Checks whether index handles specified cache row.
     *
     * @param row Cache row.
     * @return Whether index handles specified cache row
     */
    public boolean canHandle(CacheDataRow row) throws IgniteCheckedException;

    /**
     * Callback that runs when the underlying cache is updated.
     *
     * @param oldRow Cache row that was replaced with newRow.
     * @param newRow Cache row that was stored.
     * @param prevRowAvailable Whether oldRow available.
     */
    public void onUpdate(@Nullable CacheDataRow oldRow, @Nullable CacheDataRow newRow, boolean prevRowAvailable)
        throws IgniteCheckedException;

    /**
     * Provides a standard way to access the underlying concrete index
     * implementation to provide access to further, proprietary features.
     */
    public <T extends Index> T unwrap(Class<T> clazz);

    /**
     * Destroy index.
     *
     * @param softDelete if {@code true} then perform logical deletion.
     */
    public void destroy(boolean softDelete);

    /**
     * @param val Mark or unmark index to (re)build.
     */
    public void markIndexRebuild(boolean val);

    /**
     * @return Whether index is (re)building now.
     */
    public boolean rebuildInProgress();
}
