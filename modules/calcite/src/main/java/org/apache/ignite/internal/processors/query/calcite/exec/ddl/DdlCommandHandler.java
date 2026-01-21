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

package org.apache.ignite.internal.processors.query.calcite.exec.ddl;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheContextInfo;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryEntityEx;
import org.apache.ignite.internal.processors.query.QueryField;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.prepare.ddl.AlterTableAddCommand;
import org.apache.ignite.internal.processors.query.calcite.prepare.ddl.AlterTableDropCommand;
import org.apache.ignite.internal.processors.query.calcite.prepare.ddl.ColumnDefinition;
import org.apache.ignite.internal.processors.query.calcite.prepare.ddl.CreateTableCommand;
import org.apache.ignite.internal.processors.query.calcite.prepare.ddl.DdlCommand;
import org.apache.ignite.internal.processors.query.calcite.prepare.ddl.DropTableCommand;
import org.apache.ignite.internal.processors.query.calcite.prepare.ddl.NativeCommandWrapper;
import org.apache.ignite.internal.processors.query.calcite.prepare.ddl.TransactionCommand;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteCacheTable;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.schema.SchemaOperationException;
import org.apache.ignite.internal.processors.query.schema.management.SchemaManager;
import org.apache.ignite.internal.processors.query.schema.management.TableDescriptor;
import org.apache.ignite.internal.processors.security.IgniteSecurity;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.security.SecurityPermission;

import static org.apache.ignite.internal.processors.query.QueryUtils.convert;
import static org.apache.ignite.internal.processors.query.QueryUtils.isDdlOnSchemaSupported;

/** */
public class DdlCommandHandler {
    /** */
    private final GridQueryProcessor qryProc;

    /** */
    private final GridCacheProcessor cacheProc;

    /** */
    private final IgniteSecurity security;

    /** */
    private final Supplier<SchemaPlus> schemaSupp;

    /** */
    private final NativeCommandHandler nativeCmdHnd;

    /** */
    private final SchemaManager schemaMgr;

    /** */
    public DdlCommandHandler(GridQueryProcessor qryProc, GridCacheProcessor cacheProc,
        IgniteSecurity security, Supplier<SchemaPlus> schemaSupp) {
        this.qryProc = qryProc;
        this.cacheProc = cacheProc;
        this.security = security;
        this.schemaSupp = schemaSupp;
        schemaMgr = qryProc.schemaManager();
        nativeCmdHnd = new NativeCommandHandler(cacheProc.context().kernalContext());
    }

    /** */
    public void handle(UUID qryId, DdlCommand cmd) throws IgniteCheckedException {
        try {
            if (cmd instanceof TransactionCommand)
                return;

            if (cmd instanceof CreateTableCommand)
                handle0((CreateTableCommand)cmd);
            else if (cmd instanceof DropTableCommand)
                handle0((DropTableCommand)cmd);
            else if (cmd instanceof AlterTableAddCommand)
                handle0((AlterTableAddCommand)cmd);
            else if (cmd instanceof AlterTableDropCommand)
                handle0((AlterTableDropCommand)cmd);
            else if (cmd instanceof NativeCommandWrapper)
                nativeCmdHnd.handle(qryId, (NativeCommandWrapper)cmd);
            else {
                throw new IgniteSQLException("Unsupported DDL operation [" +
                    "cmdName=" + (cmd == null ? null : cmd.getClass().getSimpleName()) + "; " +
                    "cmd=\"" + cmd + "\"]", IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
            }
        }
        catch (SchemaOperationException e) {
            throw convert(e);
        }
    }

    /** */
    private void handle0(CreateTableCommand cmd) throws IgniteCheckedException {
        security.authorize(cmd.cacheName(), SecurityPermission.CACHE_CREATE);

        isDdlOnSchemaSupported(cmd.schemaName());

        if (schemaSupp.get().getSubSchema(cmd.schemaName()).getTable(cmd.tableName()) != null) {
            if (cmd.ifNotExists())
                return;

            throw new SchemaOperationException(SchemaOperationException.CODE_TABLE_EXISTS, cmd.tableName());
        }

        CacheConfiguration<?, ?> ccfg = new CacheConfiguration<>(cmd.tableName());

        QueryEntity e = toQueryEntity(cmd);

        ccfg.setQueryEntities(Collections.singleton(e));
        ccfg.setSqlSchema(cmd.schemaName());

        SchemaOperationException err =
            QueryUtils.checkQueryEntityConflicts(ccfg, cacheProc.cacheDescriptors().values());

        if (err != null)
            throw convert(err);

        if (!F.isEmpty(cmd.cacheName()) && cacheProc.cacheDescriptor(cmd.cacheName()) != null) {
            qryProc.dynamicAddQueryEntity(
                cmd.cacheName(),
                cmd.schemaName(),
                e,
                null,
                true
            ).get();
        }
        else {
            qryProc.dynamicTableCreate(
                cmd.schemaName(),
                e,
                cmd.templateName(),
                cmd.cacheName(),
                cmd.cacheGroup(),
                cmd.dataRegionName(),
                cmd.affinityKey(),
                cmd.atomicityMode(),
                cmd.writeSynchronizationMode(),
                cmd.backups(),
                cmd.ifNotExists(),
                cmd.encrypted(),
                null
            );
        }
    }

    /** */
    private void handle0(DropTableCommand cmd) throws IgniteCheckedException {
        isDdlOnSchemaSupported(cmd.schemaName());

        Table tbl = schemaSupp.get().getSubSchema(cmd.schemaName()).getTable(cmd.tableName());

        if (tbl == null) {
            if (!cmd.ifExists())
                throw new SchemaOperationException(SchemaOperationException.CODE_TABLE_NOT_FOUND, cmd.tableName());

            return;
        }

        assert tbl instanceof IgniteCacheTable : tbl;

        String cacheName = ((IgniteCacheTable)tbl).descriptor().cacheInfo().name();

        security.authorize(cacheName, SecurityPermission.CACHE_DESTROY);

        qryProc.dynamicTableDrop(cacheName, cmd.tableName(), cmd.ifExists());
    }

    /** */
    private void handle0(AlterTableAddCommand cmd) throws IgniteCheckedException {
        isDdlOnSchemaSupported(cmd.schemaName());

        TableDescriptor tblDesc = schemaMgr.table(cmd.schemaName(), cmd.tableName());

        if (tblDesc == null) {
            if (!cmd.ifTableExists())
                throw new SchemaOperationException(SchemaOperationException.CODE_TABLE_NOT_FOUND, cmd.tableName());
        }
        else {
            GridQueryTypeDescriptor typeDesc = tblDesc.type();

            if (QueryUtils.isSqlType(typeDesc.valueClass())) {
                throw new SchemaOperationException("Cannot add column(s) because table was created " +
                    "with WRAP_VALUE=false option.");
            }

            List<QueryField> cols = new ArrayList<>(cmd.columns().size());

            boolean allFieldsNullable = true;

            for (ColumnDefinition col : cmd.columns()) {
                if (typeDesc.fields().containsKey(col.name())) {
                    if (!cmd.ifColumnNotExists())
                        throw new SchemaOperationException(SchemaOperationException.CODE_COLUMN_EXISTS, col.name());
                    else
                        continue;
                }

                Type javaType = Commons.typeFactory().getResultClass(col.type());

                String typeName = javaType instanceof Class ? ((Class<?>)javaType).getName() : javaType.getTypeName();

                Integer precession = col.precision();
                Integer scale = col.scale();

                QueryField field = new QueryField(col.name(), typeName,
                    col.type().isNullable(), col.defaultValue(),
                    precession == null ? -1 : precession, scale == null ? -1 : scale);

                cols.add(field);

                allFieldsNullable &= field.isNullable();
            }

            if (!F.isEmpty(cols)) {
                GridCacheContextInfo<?, ?> ctxInfo = tblDesc.cacheInfo();

                assert ctxInfo != null;

                if (!allFieldsNullable)
                    QueryUtils.checkNotNullAllowed(ctxInfo.config());

                qryProc.dynamicColumnAdd(ctxInfo.name(), cmd.schemaName(),
                    typeDesc.tableName(), cols, cmd.ifTableExists(), cmd.ifColumnNotExists()).get();
            }
        }
    }

    /** */
    private void handle0(AlterTableDropCommand cmd) throws IgniteCheckedException {
        isDdlOnSchemaSupported(cmd.schemaName());

        TableDescriptor tblDesc = schemaMgr.table(cmd.schemaName(), cmd.tableName());

        if (tblDesc == null) {
            if (!cmd.ifTableExists())
                throw new SchemaOperationException(SchemaOperationException.CODE_TABLE_NOT_FOUND, cmd.tableName());
        }
        else {
            GridQueryTypeDescriptor typeDesc = tblDesc.type();
            GridCacheContextInfo<?, ?> ctxInfo = tblDesc.cacheInfo();

            GridCacheContext<?, ?> cctx = ctxInfo.cacheContext();

            assert cctx != null;

            if (QueryUtils.isSqlType(typeDesc.valueClass())) {
                throw new SchemaOperationException("Cannot drop column(s) because table was created " +
                    "with WRAP_VALUE=false option.");
            }

            List<String> cols = new ArrayList<>(cmd.columns().size());

            for (String colName : cmd.columns()) {
                if (!typeDesc.fields().containsKey(colName)) {
                    if (!cmd.ifColumnExists())
                        throw new SchemaOperationException(SchemaOperationException.CODE_COLUMN_NOT_FOUND, colName);
                    else
                        continue;
                }

                SchemaOperationException err = QueryUtils.validateDropColumn(typeDesc, colName);

                if (err != null)
                    throw err;

                cols.add(colName);
            }

            if (!F.isEmpty(cols)) {
                qryProc.dynamicColumnRemove(ctxInfo.name(), cmd.schemaName(),
                    typeDesc.tableName(), cols, cmd.ifTableExists(), cmd.ifColumnExists()).get();
            }
        }
    }

    /** */
    private QueryEntity toQueryEntity(CreateTableCommand cmd) {
        QueryEntity res = new QueryEntity();

        res.setTableName(cmd.tableName());

        Set<String> notNullFields = null;

        HashMap<String, Object> dfltValues = new HashMap<>();

        Map<String, Integer> precision = new HashMap<>();
        Map<String, Integer> scale = new HashMap<>();

        IgniteTypeFactory tf = Commons.typeFactory();

        for (ColumnDefinition col : cmd.columns()) {
            String name = col.name();

            String typeName = typeName(tf, col.type());

            res.addQueryField(name, typeName, null);

            if (!col.nullable()) {
                if (notNullFields == null)
                    notNullFields = new HashSet<>();

                notNullFields.add(name);
            }

            if (col.defaultValue() != null)
                dfltValues.put(name, col.defaultValue());

            if (col.precision() != null)
                precision.put(name, col.precision());

            if (col.scale() != null)
                scale.put(name, col.scale());
        }

        if (!F.isEmpty(dfltValues))
            res.setDefaultFieldValues(dfltValues);

        if (!F.isEmpty(precision))
            res.setFieldsPrecision(precision);

        if (!F.isEmpty(scale))
            res.setFieldsScale(scale);

        String valTypeName = QueryUtils.createTableValueTypeName(cmd.schemaName(), cmd.tableName());

        String keyTypeName;
        if ((!F.isEmpty(cmd.primaryKeyColumns()) && cmd.primaryKeyColumns().size() > 1) || !F.isEmpty(cmd.keyTypeName())) {
            keyTypeName = cmd.keyTypeName();

            if (F.isEmpty(keyTypeName))
                keyTypeName = QueryUtils.createTableKeyTypeName(valTypeName);

            if (!F.isEmpty(cmd.primaryKeyColumns())) {
                res.setKeyFields(new LinkedHashSet<>(cmd.primaryKeyColumns()));

                res = new QueryEntityEx(res).setPreserveKeysOrder(true);
            }
        }
        else if (!F.isEmpty(cmd.primaryKeyColumns()) && cmd.primaryKeyColumns().size() == 1) {
            String pkFieldName = cmd.primaryKeyColumns().get(0);

            keyTypeName = res.getFields().get(pkFieldName);

            res.setKeyFieldName(pkFieldName);
        }
        else {
            // if pk is not explicitly set, we create it ourselves
            keyTypeName = IgniteUuid.class.getName();

            res = new QueryEntityEx(res).implicitPk(true);
        }

        res.setValueType(F.isEmpty(cmd.valueTypeName()) ? valTypeName : cmd.valueTypeName());
        res.setKeyType(keyTypeName);

        if (!F.isEmpty(notNullFields)) {
            QueryEntityEx res0 = new QueryEntityEx(res);

            res0.setNotNullFields(notNullFields);

            res = res0;
        }

        return res;
    }

    /** Creates type name. Includes component types if the type is an array. */
    private static String typeName(IgniteTypeFactory tf, RelDataType type) {
        Type javaType = tf.getResultClass(type);

        String typeName = javaType instanceof Class ? ((Class<?>)javaType).getName() : javaType.getTypeName();

        // Map is not supported yet.
        if (SqlTypeUtil.isArray(type)) {
            assert type instanceof ArraySqlType;

            typeName += ':';

            typeName += typeName(tf, type.getComponentType());
        }

        return typeName;
    }
}
