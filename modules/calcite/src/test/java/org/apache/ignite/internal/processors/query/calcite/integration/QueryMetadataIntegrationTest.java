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

package org.apache.ignite.internal.processors.query.calcite.integration;

import java.math.BigDecimal;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.T2;
import org.junit.Test;
import org.mockito.Mockito;

import static org.apache.calcite.rel.type.RelDataType.PRECISION_NOT_SPECIFIED;
import static org.apache.calcite.rel.type.RelDataType.SCALE_NOT_SPECIFIED;

/**
 * Test query metadata.
 */
public class QueryMetadataIntegrationTest extends AbstractBasicIntegrationTest {
    /** */
    @Test
    public void testJoin() throws Exception {
        executeSql("CREATE TABLE tbl1 (id DECIMAL(10, 2), val VARCHAR, val2 BIGINT, ts TIMESTAMP(14), PRIMARY KEY(id, val))");
        executeSql("CREATE TABLE tbl2 (id DECIMAL(10, 2), val VARCHAR, val2 BIGINT, ts TIMESTAMP(14), PRIMARY KEY(id, val))");

        checker("select * from tbl1 inner join tbl2 on (tbl1.id > tbl2.id and tbl1.id <> ?) " +
            "where tbl1.id > ? and tbl2.val like ? or tbl1.ts = ?")
            .addMeta(
                builder -> builder
                    .add("PUBLIC", "TBL1", BigDecimal.class, "ID", 10, 2, false)
                    .add("PUBLIC", "TBL1", String.class, "VAL", true)
                    .add("PUBLIC", "TBL1", Long.class, "VAL2", 19, 0, true)
                    .add("PUBLIC", "TBL1", java.sql.Timestamp.class, "TS", 0, SCALE_NOT_SPECIFIED, true)
                    .add("PUBLIC", "TBL2", BigDecimal.class, "ID", 10, 2, false)
                    .add("PUBLIC", "TBL2", String.class, "VAL", true)
                    .add("PUBLIC", "TBL2", Long.class, "VAL2", 19, 0, true)
                    .add("PUBLIC", "TBL2", java.sql.Timestamp.class, "TS", 0, SCALE_NOT_SPECIFIED, true),
                builder -> builder
                    .add(BigDecimal.class, 10, 2)
                    .add(BigDecimal.class, 10, 2)
                    .add(String.class)
                    .add(java.sql.Timestamp.class, 0, SCALE_NOT_SPECIFIED)
            ).check();
    }

    /** */
    @Test
    public void testMultipleConditions() throws Exception {
        executeSql("CREATE TABLE tbl (id BIGINT, val VARCHAR, PRIMARY KEY(id))");
        executeSql("CREATE INDEX tbl_val_idx ON tbl(val)");

        checker("select * from tbl where id in (?, ?) or (id > ? and id <= ?) or (val <> ?)")
            .addMeta(
                builder -> builder
                    .add("PUBLIC", "TBL", Long.class, "ID", 19, 0, true)
                    .add("PUBLIC", "TBL", String.class, "VAL", true),
                builder -> builder
                    .add(Long.class, 19, 0)
                    .add(Long.class, 19, 0)
                    .add(Long.class, 19, 0)
                    .add(Long.class, 19, 0)
                    .add(String.class)
            ).check();
    }

    /** */
    @Test
    public void testMultipleQueries() throws Exception {
        executeSql("CREATE TABLE tbl (id BIGINT, val VARCHAR, PRIMARY KEY(id))");
        executeSql("CREATE INDEX tbl_val_idx ON tbl(val)");

        checker("insert into tbl(id, val) values (?, ?); select * from tbl where id > ?")
            .addMeta(
                builder -> builder
                    .add(null, null, long.class, "ROWCOUNT", 19, 0, false),
                builder -> builder
                    .add(Long.class, 19, 0)
                    .add(String.class)
            )
            .addMeta(
                builder -> builder
                    .add("PUBLIC", "TBL", Long.class, "ID", 19, 0, true)
                    .add("PUBLIC", "TBL", String.class, "VAL", true),
                builder -> builder
                    .add(Long.class, 19, 0)
            )
            .check();
    }

    /** */
    @Test
    public void testDml() throws Exception {
        executeSql("CREATE TABLE tbl1 (id BIGINT, val VARCHAR, PRIMARY KEY(id))");
        executeSql("CREATE TABLE tbl2 (id BIGINT, val VARCHAR, PRIMARY KEY(id))");

        checker("insert into tbl1(id, val) values (?, ?)")
            .addMeta(
                builder -> builder
                    .add(null, null, long.class, "ROWCOUNT", 19, 0, false),
                builder -> builder
                    .add(Long.class, 19, 0)
                    .add(String.class)
            )
            .check();

        checker("insert into tbl2(id) select id from tbl1 where id > ? and val in (?, ?)")
            .addMeta(
                builder -> builder
                    .add(null, null, long.class, "ROWCOUNT", 19, 0, false),
                builder -> builder
                    .add(Long.class, 19, 0)
                    .add(String.class)
                    .add(String.class)
            )
            .check();

        checker("update tbl2 set val = ? where id in (?, ?)")
            .addMeta(
                builder -> builder
                    .add(null, null, long.class, "ROWCOUNT", 19, 0, false),
                builder -> builder
                    .add(String.class)
                    .add(Long.class, 19, 0)
                    .add(Long.class, 19, 0)
            )
            .check();
    }

    /** */
    @Test
    public void testDdl() throws Exception {
        executeSql("CREATE TABLE tbl1 (id BIGINT, val VARCHAR, PRIMARY KEY(id))");

        checker("CREATE TABLE tbl2 (id BIGINT, val VARCHAR, PRIMARY KEY(id))")
            .addMeta(builder -> {}, builder -> {})
            .check();

        // Metadata of create as select is always empty, because it is impossible to validate insert
        // query without creating table.
        checker("CREATE TABLE tbl2 (id, val) AS SELECT id, val FROM tbl1 WHERE id > ?")
            .addMeta(builder -> {}, builder -> {})
            .check();
    }

    /** */
    @Test
    public void testExplain() throws Exception {
        executeSql("CREATE TABLE tbl (id BIGINT, val VARCHAR, PRIMARY KEY(id))");

        checker("explain plan for select * from tbl where id in (?, ?) or (id > ? and id <= ?) or (val <> ?)")
            .addMeta(
                builder -> builder
                    .add(null, null, String.class, "PLAN", false),
                builder -> {}
            ).check();
    }

    /** */
    private MetadataChecker checker(String sql) {
        return new MetadataChecker(queryEngine(client), sql);
    }

    /** */
    private static class MetadataChecker {
        /** */
        private final QueryEngine qryEngine;

        /** */
        private final String sql;

        /** */
        private final String schema;

        /** */
        private final List<T2<List<GridQueryFieldMetadata>, List<GridQueryFieldMetadata>>> expected = new ArrayList<>();

        /** */
        private int paramOffset;

        /** */
        public MetadataChecker(QueryEngine qryEngine, String sql) {
            this(qryEngine, sql, "PUBLIC");
        }

        /** */
        public MetadataChecker(QueryEngine qryEngine, String sql, String schema) {
            this.qryEngine = qryEngine;
            this.sql = sql;
            this.schema = schema;
        }

        /** */
        public MetadataChecker addMeta(Consumer<MetadataBuilder> resultMeta, Consumer<MetadataBuilder> paramMeta) {
            MetadataBuilder rmBuilder = new MetadataBuilder();
            resultMeta.accept(rmBuilder);

            MetadataBuilder prmBuilder = new MetadataBuilder(paramOffset);
            paramMeta.accept(prmBuilder);
            List<GridQueryFieldMetadata> paramMetaRes = prmBuilder.build();
            paramOffset += paramMetaRes.size();

            expected.add(new T2<>(rmBuilder.build(), paramMetaRes));

            return this;
        }

        /** */
        public void check() throws Exception {
            List<T2<List<GridQueryFieldMetadata>, List<GridQueryFieldMetadata>>> actual =
                qryEngine.queryMetadata(null, schema, sql);

            assertEquals(expected.size(), actual.size());

            for (int i = 0; i < actual.size(); ++i) {
                checkMeta(expected.get(i).getKey(), actual.get(i).getKey());
                checkMeta(expected.get(i).getValue(), actual.get(i).getValue());
            }
        }

        /** */
        private void checkMeta(List<GridQueryFieldMetadata> exp, List<GridQueryFieldMetadata> actual) {
            assertEquals(exp.size(), actual.size());

            for (int i = 0; i < actual.size(); ++i) {
                GridQueryFieldMetadata actualMeta = actual.get(i);
                GridQueryFieldMetadata expMeta = exp.get(i);

                assertEquals(expMeta.schemaName(), actualMeta.schemaName());
                assertEquals(expMeta.typeName(), actualMeta.typeName());
                assertEquals(expMeta.fieldName(), actualMeta.fieldName());
                assertEquals(expMeta.fieldTypeName(), actualMeta.fieldTypeName());
                assertEquals(expMeta.nullability(), actualMeta.nullability());
                assertEquals(expMeta.scale(), actualMeta.scale());
                assertEquals(expMeta.precision(), actualMeta.precision());
            }
        }
    }

    /** */
    private static class MetadataBuilder {
        /** */
        private final List<GridQueryFieldMetadata> fields = new ArrayList<>();

        /** */
        private final int fieldOffset;

        /** */
        MetadataBuilder() {
            this(0);
        }

        /** */
        MetadataBuilder(int fieldOffset) {
            this.fieldOffset = fieldOffset;
        }

        /** */
        MetadataBuilder add(Class<?> fieldClass) {
            add(fieldClass, PRECISION_NOT_SPECIFIED, SCALE_NOT_SPECIFIED);
            return this;
        }

        /** */
        MetadataBuilder add(Class<?> fieldClass, int precision, int scale) {
            add(null, null, fieldClass, "?" + (fields.size() + fieldOffset), precision, scale, true);
            return this;
        }

        /** */
        MetadataBuilder add(String schemaName, String typeName, Class<?> fieldClass, String fieldName) {
            add(schemaName, typeName, fieldClass, fieldName, true);
            return this;
        }

        /** */
        MetadataBuilder add(
            String schemaName,
            String typeName,
            Class<?> fieldClass,
            String fieldName,
            boolean isNullable
        ) {
            add(schemaName, typeName, fieldClass, fieldName, PRECISION_NOT_SPECIFIED, SCALE_NOT_SPECIFIED, isNullable);
            return this;
        }

        /** */
        MetadataBuilder add(
            String schemaName,
            String typeName,
            Class<?> fieldClass,
            String fieldName,
            int precision,
            int scale,
            boolean isNullable
        ) {
            GridQueryFieldMetadata mocked = Mockito.mock(GridQueryFieldMetadata.class);

            Mockito.when(mocked.schemaName()).thenReturn(schemaName);
            Mockito.when(mocked.typeName()).thenReturn(typeName);
            Mockito.when(mocked.fieldName()).thenReturn(fieldName);
            Mockito.when(mocked.fieldTypeName()).thenReturn(fieldClass.getName());
            Mockito.when(mocked.precision()).thenReturn(precision);
            Mockito.when(mocked.scale()).thenReturn(scale);
            Mockito.when(mocked.nullability())
                .thenReturn(isNullable ? ResultSetMetaData.columnNullable : ResultSetMetaData.columnNoNulls);

            fields.add(mocked);

            return this;
        }

        /** */
        List<GridQueryFieldMetadata> build() {
            return Collections.unmodifiableList(fields);
        }
    }

    /** */
    private QueryEngine queryEngine(IgniteEx ign) {
        return Commons.lookupComponent(ign.context(), QueryEngine.class);
    }
}
