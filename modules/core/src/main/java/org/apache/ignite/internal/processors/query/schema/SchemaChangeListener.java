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

import java.lang.reflect.Method;
import java.util.List;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.QueryField;
import org.apache.ignite.internal.processors.query.schema.management.IndexDescriptor;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public interface SchemaChangeListener {
    /**
     * Callback method.
     *
     * @param schemaName Schema name.
     */
    public void onSchemaCreated(String schemaName);

    /**
     * Callback method.
     *
     * @param schemaName Schema name.
     */
    public void onSchemaDropped(String schemaName);

    /**
     * Callback method.
     *
     * @param schemaName Schema name.
     * @param typeDesc Type descriptor.
     * @param cacheInfo Cache info.
     */
    public void onSqlTypeCreated(
        String schemaName,
        GridQueryTypeDescriptor typeDesc,
        GridCacheContextInfo<?, ?> cacheInfo
    );

    /**
     * Callback on columns added.
     *
     * @param schemaName Schema name.
     * @param typeDesc Type descriptor.
     * @param cacheInfo Cache info.
     * @param cols Added columns' names.
     */
    public void onColumnsAdded(
        String schemaName,
        GridQueryTypeDescriptor typeDesc,
        GridCacheContextInfo<?, ?> cacheInfo,
        List<QueryField> cols
    );

    /**
     * Callback on columns dropped.
     *
     * @param schemaName Schema name.
     * @param typeDesc Type descriptor.
     * @param cacheInfo Cache info.
     * @param cols Dropped columns' names.
     */
    public void onColumnsDropped(
        String schemaName,
        GridQueryTypeDescriptor typeDesc,
        GridCacheContextInfo<?, ?> cacheInfo,
        List<String> cols
    );

    /**
     * Callback method.
     *
     * @param schemaName Schema name.
     * @param typeDesc Type descriptor.
     * @param destroy Cache destroy flag.
     */
    public void onSqlTypeDropped(String schemaName, GridQueryTypeDescriptor typeDesc, boolean destroy);

    /**
     * Callback on index creation.
     *
     * @param schemaName Schema name.
     * @param tblName Table name.
     * @param idxName Index name.
     * @param idxDesc Index descriptor.
     */
    public void onIndexCreated(String schemaName, String tblName, String idxName, IndexDescriptor idxDesc);

    /**
     * Callback on index drop.
     *
     * @param schemaName Schema name.
     * @param tblName Table name.
     * @param idxName Index name.
     */
    public void onIndexDropped(String schemaName, String tblName, String idxName);

    /**
     * Callback on index rebuild started for all indexes in the table.
     *
     * @param schemaName Schema name.
     * @param tblName Table name.
     */
    public void onIndexRebuildStarted(String schemaName, String tblName);

    /**
     * Callback on index rebuild finished for all indexes in the table.
     *
     * @param schemaName Schema name.
     * @param tblName Table name.
     */
    public void onIndexRebuildFinished(String schemaName, String tblName);

    /**
     * Callback on function creation.
     *
     * @param schemaName Schema name.
     * @param name Function name.
     * @param deterministic Specifies if the function is deterministic (result depends only on input parameters)
     * @param method Public static method, implementing this function.
     */
    public void onFunctionCreated(String schemaName, String name, boolean deterministic, Method method);

    /**
     * Callback on table function creation.
     *
     * @param schemaName Schema name.
     * @param name Function name.
     * @param colTypes Column types of the returned table.
     * @param colNames Column names if the returned table. If {@code null} or empty, the default names are used instead.
     * @param method Public static method, implementing this function.
     */
    public void onTableFunctionCreated(String schemaName, String name, Method method, Class<?>[] colTypes,
        @Nullable String[] colNames);

    /**
     * Callback on system view creation.
     *
     * @param schemaName Schema name.
     * @param sysView System view.
     */
    public void onSystemViewCreated(String schemaName, SystemView<?> sysView);

    /**
     * Callback on user defined view creation.
     *
     * @param schemaName Schema name.
     * @param viewName View name.
     * @param viewSql View SQL.
     */
    public void onViewCreated(String schemaName, String viewName, String viewSql);

    /**
     * Callback on user defined view dropped.
     *
     * @param schemaName Schema name.
     * @param viewName View name.
     */
    public void onViewDropped(String schemaName, String viewName);
}
