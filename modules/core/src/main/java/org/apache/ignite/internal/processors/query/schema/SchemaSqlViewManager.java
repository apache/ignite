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

import java.io.Serializable;
import java.util.Objects;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cluster.IgniteChangeGlobalStateSupport;
import org.apache.ignite.internal.processors.metastorage.DistributedMetaStorage;
import org.apache.ignite.internal.processors.metastorage.DistributedMetastorageLifecycleListener;
import org.apache.ignite.internal.processors.metastorage.ReadableDistributedMetaStorage;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.jetbrains.annotations.Nullable;

/**
 * Global schema SQL view manager.
 */
public class SchemaSqlViewManager implements IgniteChangeGlobalStateSupport {
    /** Distributed metastorage key prefix. */
    private static final String SQL_VIEW_KEY_PREFIX = DistributedMetaStorage.IGNITE_INTERNAL_KEY_PREFIX + "sql.view.";

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** */
    private volatile DistributedMetaStorage metastorage;

    /** Logger. */
    private final IgniteLogger log;

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */
    public SchemaSqlViewManager(GridKernalContext ctx) {
        this.ctx = ctx;

        log = ctx.log(SchemaSqlViewManager.class);
    }

    /** */
    public void start() {
        ctx.internalSubscriptionProcessor().registerDistributedMetastorageListener(new DistributedMetastorageLifecycleListener() {
            @Override public void onReadyForRead(ReadableDistributedMetaStorage metastorage) {
                metastorage.listen(key -> key.startsWith(SQL_VIEW_KEY_PREFIX),
                    SchemaSqlViewManager.this::processMetastorageUpdate);
            }

            @Override public void onReadyForWrite(DistributedMetaStorage metastorage) {
                SchemaSqlViewManager.this.metastorage = metastorage;
            }
        });

        ctx.internalSubscriptionProcessor().registerGlobalStateListener(this);
    }

    /** */
    public void createView(String schemaName, String viewName, String viewSql, boolean replace) throws IgniteCheckedException {
        ctx.security().authorize(SecurityPermission.SQL_VIEW_CREATE);

        if (!ctx.query().schemaManager().schemaNames().contains(schemaName))
            throw new SchemaOperationException(SchemaOperationException.CODE_SCHEMA_NOT_FOUND, schemaName);

        String key = makeKey(schemaName, viewName);

        Serializable oldVal;
        Serializable newVal = new SqlView(schemaName, viewName, viewSql);

        do {
            oldVal = metastorage.read(key);

            if (oldVal != null && !replace)
                throw new SchemaOperationException(SchemaOperationException.CODE_VIEW_EXISTS, viewName);
        }
        while (!metastorage.compareAndSet(key, oldVal, newVal));
    }

    /** */
    public void dropView(String schemaName, String viewName, boolean ifExists) throws IgniteCheckedException {
        ctx.security().authorize(SecurityPermission.SQL_VIEW_DROP);

        String key = makeKey(schemaName, viewName);

        Serializable oldVal;

        do {
            oldVal = metastorage.read(key);

            if (oldVal == null) {
                if (!ifExists)
                    throw new SchemaOperationException(SchemaOperationException.CODE_VIEW_NOT_FOUND, viewName);
                else
                    return;
            }
        }
        while (!metastorage.compareAndRemove(key, oldVal));
    }

    /** */
    public void clearSchemaViews(String schemaName) {
        if (!U.isLocalNodeCoordinator(ctx.discovery()))
            return;

        try {
            metastorage.iterate(makeKey(schemaName, null), (k, v) -> {
                try {
                    metastorage.removeAsync(k);
                }
                catch (IgniteCheckedException e) {
                    log.warning("Failed to remove SQL views from metastorage [key=" + k + ']', e);
                }
            });
        }
        catch (IgniteCheckedException e) {
            log.warning("Failed to get views list from metastorage", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void onActivate(GridKernalContext kctx) throws IgniteCheckedException {
        metastorage.iterate(SQL_VIEW_KEY_PREFIX, (k, v) -> processMetastorageUpdate(k, null, v));
    }

    /** {@inheritDoc} */
    @Override public void onDeActivate(GridKernalContext kctx) {
        // No-op.
    }

    /** */
    private void processMetastorageUpdate(String key, @Nullable Serializable oldVal, @Nullable Serializable newVal) {
        assert key.startsWith(SQL_VIEW_KEY_PREFIX) : "Invalid key: " + key;
        assert oldVal == null || oldVal instanceof SqlView : "Invalid old value: " + oldVal;
        assert newVal == null || newVal instanceof SqlView : "Invalid new value: " + newVal;
        assert oldVal != null || newVal != null;

        String schemaName = newVal == null ? ((SqlView)oldVal).schemaName : ((SqlView)newVal).schemaName;
        String viewName = newVal == null ? ((SqlView)oldVal).viewName : ((SqlView)newVal).viewName;

        // Register in local manager.
        if (newVal == null)
            ctx.query().schemaManager().dropView(schemaName, viewName);
        else
            ctx.query().schemaManager().createView(schemaName, viewName, ((SqlView)newVal).viewSql);
    }

    /** */
    private static String makeKey(String schemaName, @Nullable String viewName) {
        return SQL_VIEW_KEY_PREFIX + escape(schemaName) + '.' + (viewName == null ? "" : escape(viewName));
    }

    /** */
    private static String escape(String name) {
        return name.replace("\\", "\\\\").replace(".", "\\.");
    }

    /** */
    private static class SqlView implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final String schemaName;

        /** */
        private final String viewName;

        /** */
        private final String viewSql;

        /** */
        private SqlView(String schemaName, String viewName, String viewSql) {
            this.schemaName = schemaName;
            this.viewName = viewName;
            this.viewSql = viewSql;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof SqlView))
                return false;

            SqlView view = (SqlView)o;

            return Objects.equals(schemaName, view.schemaName) &&
                Objects.equals(viewName, view.viewName) &&
                Objects.equals(viewSql, view.viewSql);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(schemaName, viewName, viewSql);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(SqlView.class, this);
        }
    }
}
