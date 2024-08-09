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

package org.apache.ignite.internal.processors.query.calcite.schema;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.prepare.BaseQueryContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgnitePlanner;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.ValidationResult;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Ignite SQL view table macro implementation.
 */
public class ViewTableMacroImpl implements TableMacro {
    /** */
    private static final ThreadLocal<Set<ViewTableMacroImpl>> stack = ThreadLocal.withInitial(HashSet::new);

    /** */
    private final String viewSql;

    /** */
    private final SchemaPlus schema;

    /** Ctor. */
    public ViewTableMacroImpl(String viewSql, SchemaPlus schema) {
        this.viewSql = viewSql;
        this.schema = schema;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("resource")
    @Override public TranslatableTable apply(List<?> arguments) {
        if (!stack.get().add(this))
            throw new IgniteSQLException("Recursive views are not supported: " + viewSql);

        try {
            IgnitePlanner planner = PlanningContext.builder()
                .parentContext(BaseQueryContext.builder().defaultSchema(schema).build())
                .build()
                .planner();

            SqlNode sqlNode = planner.parse(viewSql);
            ValidationResult res = planner.validateAndGetTypeMetadata(sqlNode);

            Map<String, List<String>> origins = new HashMap<>();

            if (!F.isEmpty(res.origins())) {
                for (int i = 0; i < res.origins().size(); i++) {
                    List<String> origin = res.origins().get(i);

                    if (!F.isEmpty(origin))
                        origins.put(res.dataType().getFieldList().get(i).getName(), origin);
                }
            }

            return new ViewTableImpl(planner.getTypeFactory().getJavaClass(res.dataType()),
                RelDataTypeImpl.proto(res.dataType()), viewSql, CalciteSchema.from(schema).path(null), origins);
        }
        catch (IgniteSQLException e) {
            throw e;
        }
        catch (Exception e) {
            throw new IgniteSQLException("Failed to validate SQL view query. " + e.getMessage(),
                IgniteQueryErrorCode.PARSING, e);
        }
        finally {
            stack.get().remove(this);
        }
    }

    /** {@inheritDoc} */
    @Override public List<FunctionParameter> getParameters() {
        return Collections.emptyList();
    }
}
