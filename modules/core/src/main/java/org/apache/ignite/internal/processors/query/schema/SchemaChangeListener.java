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

package org.apache.ignite.internal.processors.query.schema;

import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.query.GridIndex;
import org.apache.ignite.internal.processors.query.GridQueryIndexDescriptor;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;

/**
 *
 */
public interface SchemaChangeListener {
    /**
     * Callback method.
     *
     * @param schemaName Schema name.
     */
    void onSchemaCreate(String schemaName);

    /**
     * Callback method.
     *
     * @param schemaName Schema name.
     */
    void onSchemaDrop(String schemaName);

    /**
     * Callback method.
     * @param schemaName Schema name.
     * @param typeDesc type descriptor.
     * @param cacheInfo Cache info.
     * @param pk Primary key index
     */
    void onSqlTypeCreate(String schemaName, GridQueryTypeDescriptor typeDesc,
        GridCacheContextInfo<?, ?> cacheInfo, GridIndex<?> pk);

    /**
     * Callback method.
     *
     * @param schemaName Schema name.
     * @param typeDesc type descriptor.
     */
    void onSqlTypeDrop(String schemaName, GridQueryTypeDescriptor typeDesc);

    /**
     * Callback on index creation.
     *
     * @param schemaName Schema name.
     * @param tblName Table name.
     * @param idxName Index name.
     * @param idxDesc Index descriptor.
     * @param idx Index.
     */
    void onIndexCreate(String schemaName, String tblName, String idxName, GridQueryIndexDescriptor idxDesc,
        GridIndex<?> idx);

    /**
     * Callback on index drop.
     *
     * @param schemaName Schema name.
     * @param tblName Table name.
     * @param idxName Index name.
     */
    void onIndexDrop(String schemaName, String tblName, String idxName);
}
