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
 * Abstract schema change listener with no-op implementation for all calbacks.
 */
public abstract class AbstractSchemaChangeListener implements SchemaChangeListener {
    /** {@inheritDoc} */
    @Override public void onSchemaCreated(String schemaName) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onSchemaDropped(String schemaName) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onIndexCreated(
        String schemaName,
        String tblName,
        String idxName,
        IndexDescriptor idxDesc
    ) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onIndexDropped(String schemaName, String tblName, String idxName) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onIndexRebuildStarted(String schemaName, String tblName) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onIndexRebuildFinished(String schemaName, String tblName) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onSqlTypeCreated(
        String schemaName,
        GridQueryTypeDescriptor typeDesc,
        GridCacheContextInfo<?, ?> cacheInfo
    ) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onColumnsAdded(
        String schemaName,
        GridQueryTypeDescriptor typeDesc,
        GridCacheContextInfo<?, ?> cacheInfo,
        List<QueryField> cols
    ) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onColumnsDropped(
        String schemaName,
        GridQueryTypeDescriptor typeDesc,
        GridCacheContextInfo<?, ?> cacheInfo,
        List<String> cols
    ) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onSqlTypeDropped(
        String schemaName,
        GridQueryTypeDescriptor typeDescriptor,
        boolean destroy
    ) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onFunctionCreated(String schemaName, String name, boolean deterministic, Method method) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onTableFunctionCreated(String schemaName, String name, Method method, Class<?>[] colTypes,
        @Nullable String[] colNames) {

    }

    /** {@inheritDoc} */
    @Override public void onSystemViewCreated(String schemaName, SystemView<?> sysView) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onViewCreated(String schemaName, String viewName, String viewSql) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onViewDropped(String schemaName, String viewName) {
        // No-op.
    }
}
