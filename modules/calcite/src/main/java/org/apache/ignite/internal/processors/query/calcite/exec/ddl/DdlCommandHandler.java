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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryEntityEx;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.ddl.ColumnDefinition;
import org.apache.ignite.internal.processors.query.calcite.prepare.ddl.CreateTableCommand;
import org.apache.ignite.internal.processors.query.calcite.prepare.ddl.DdlCommand;
import org.apache.ignite.internal.processors.query.calcite.prepare.ddl.DropTableCommand;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.schema.SchemaOperationException;
import org.apache.ignite.internal.processors.security.IgniteSecurity;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.security.SecurityPermission;

import static org.apache.ignite.internal.processors.query.QueryUtils.convert;
import static org.apache.ignite.internal.processors.query.QueryUtils.isDdlOnSchemaSupported;

/** */
public class DdlCommandHandler {
    /** */
    private final Supplier<GridQueryProcessor> qryProcessorSupp;

    /** */
    private final GridCacheProcessor cacheProcessor;

    /** */
    private final IgniteSecurity security;

    /** */
    private final Supplier<SchemaPlus> schemaSupp;

    /** */
    public DdlCommandHandler(Supplier<GridQueryProcessor> qryProcessorSupp, GridCacheProcessor cacheProcessor,
        IgniteSecurity security, Supplier<SchemaPlus> schemaSupp) {
        this.qryProcessorSupp = qryProcessorSupp;
        this.cacheProcessor = cacheProcessor;
        this.security = security;
        this.schemaSupp = schemaSupp;
    }

    /** */
    public void handle(PlanningContext pctx, DdlCommand cmd) throws IgniteCheckedException {
        if (cmd instanceof CreateTableCommand)
            handle0(pctx, (CreateTableCommand)cmd);

        else if (cmd instanceof DropTableCommand)
            handle0(pctx, (DropTableCommand)cmd);

        else {
            throw new IgniteSQLException("Unsupported DDL operation [" +
                "cmdName=" + (cmd == null ? null : cmd.getClass().getSimpleName()) + "; " +
                "querySql=\"" + pctx.query() + "\"]", IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
        }
    }

    /** */
    private void handle0(PlanningContext pctx, CreateTableCommand cmd) throws IgniteCheckedException {
        security.authorize(cmd.cacheName(), SecurityPermission.CACHE_CREATE);

        isDdlOnSchemaSupported(cmd.schemaName());

        if (schemaSupp.get().getSubSchema(cmd.schemaName()).getTable(cmd.tableName()) != null) {
            if (cmd.ifNotExists())
                return;

            throw new SchemaOperationException(SchemaOperationException.CODE_TABLE_EXISTS, cmd.tableName());
        }

        CacheConfiguration<?, ?> ccfg = new CacheConfiguration<>(cmd.tableName());

        QueryEntity e = toQueryEntity(cmd, pctx);

        ccfg.setQueryEntities(Collections.singleton(e));
        ccfg.setSqlSchema(cmd.schemaName());

        SchemaOperationException err =
            QueryUtils.checkQueryEntityConflicts(ccfg, cacheProcessor.cacheDescriptors().values());

        if (err != null)
            throw convert(err);

        qryProcessorSupp.get().dynamicTableCreate(
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

    /** */
    private void handle0(PlanningContext pctx, DropTableCommand cmd) throws IgniteCheckedException {
        isDdlOnSchemaSupported(cmd.schemaName());

        Table tbl = schemaSupp.get().getSubSchema(cmd.schemaName()).getTable(cmd.tableName());

        if (tbl == null) {
            if (!cmd.ifExists())
                throw new SchemaOperationException(SchemaOperationException.CODE_TABLE_NOT_FOUND, cmd.tableName());

            return;
        }

        String cacheName = ((IgniteTable)tbl).descriptor().cacheInfo().name();

        security.authorize(cacheName, SecurityPermission.CACHE_DESTROY);

        qryProcessorSupp.get().dynamicTableDrop(cacheName, cmd.tableName(), cmd.ifExists());
    }

    /** */
    private QueryEntity toQueryEntity(CreateTableCommand cmd, PlanningContext pctx) {
        QueryEntity res = new QueryEntity();

        res.setTableName(cmd.tableName());

        Set<String> notNullFields = null;

        HashMap<String, Object> dfltValues = new HashMap<>();

        Map<String, Integer> precision = new HashMap<>();
        Map<String, Integer> scale = new HashMap<>();

        IgniteTypeFactory tf = pctx.typeFactory();

        for (ColumnDefinition col : cmd.columns()) {
            String name = col.name();

            Type javaType = tf.getJavaClass(col.type());

            String typeName = javaType instanceof Class ? ((Class<?>)javaType).getName() : javaType.getTypeName();

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
}
