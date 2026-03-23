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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.List;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.calcite.PseudoColumnDescriptor;
import org.apache.ignite.calcite.PseudoColumnProvider;
import org.apache.ignite.calcite.PseudoColumnValueExtractorContext;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.indexing.IndexingQueryEngineConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.PseudoColumnValueExtractorContextEx;
import org.apache.ignite.internal.processors.query.calcite.QueryChecker;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.RexImpTable;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgniteSqlCallRewriteTable;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/** */
public class RowIdPseudoColumnTest extends AbstractBasicIntegrationTest {
    /** {@inheritDoc} */
    @Override protected int nodeCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        SqlConfiguration sqlCfg = new SqlConfiguration().setQueryEnginesConfiguration(
            new CalciteQueryEngineConfiguration().setDefault(true),
            new IndexingQueryEngineConfiguration()
        );

        return super.getConfiguration(igniteInstanceName)
            .setSqlConfiguration(sqlCfg)
            .setPluginProviders(new RowIdPseudoColumnPluginProvider());
    }

    @Test
    public void testSimplePrimaryKey() {
        sql("create table PUBLIC.PERSON(id int primary key, name varchar)");

        for (int i = 0; i < 2; i++)
            sql("insert into PUBLIC.PERSON(id, name) values(?, ?)", i, "foo" + i);

        List<List<?>> selectRs = sql("select rowid from PUBLIC.PERSON order by id");
        Object rowId0 = selectRs.get(0).get(0);
        Object rowId1 = selectRs.get(1).get(0);

        assertQuery("select id, name, rowid from PUBLIC.PERSON where rowid = ?")
            .withParams(rowId0)
            .columnNames("ID", "NAME", "ROWID")
            .matches(QueryChecker.containsIndexScan("PUBLIC", "PERSON", "_key_PK"))
            .returns(0, "foo0", rowId0)
            .check();

        assertQuery("select p.id, p.name, p.rowid from PUBLIC.PERSON as p where p.rowid = ?")
            .withParams(rowId1)
            .columnNames("ID", "NAME", "ROWID")
            .matches(QueryChecker.containsIndexScan("PUBLIC", "PERSON", "_key_PK"))
            .returns(1, "foo1", rowId1)
            .check();
    }

    @Test
    public void testCompositePrimaryKey() {
        sql("create table PUBLIC.PERSON(id int, name varchar, age int, primary key (id, name))");

        for (int i = 0; i < 2; i++)
            sql("insert into PUBLIC.PERSON(id, name, age) values(?, ?, ?)", i, "foo" + i, 18 + i);

        List<List<?>> selectRs = sql("select rowid, _key from PUBLIC.PERSON order by id");
        Object rowId0 = selectRs.get(0).get(0);
        Object rowId1 = selectRs.get(1).get(0);

        // TODO: IGNITE-28223-add-rowid Вот тут проблема, для составных ПК он не умеет пока делать хитрости в начале
        //  надо будет попровить _key для бинарного объекта а потом уже сюда вернуться
        assertQuery("select id, name, age, rowid from PUBLIC.PERSON where rowid = ?")
            .withParams(rowId0)
            .columnNames("ID", "NAME", "AGE", "ROWID")
            .matches(QueryChecker.containsIndexScan("PUBLIC", "PERSON", "_key_PK"))
            .returns(0, "foo0", 18, rowId0)
            .check();

        assertQuery("select p.id, p.name, p.age, p.rowid from PUBLIC.PERSON where p.rowid = ?")
            .withParams(rowId1)
            .columnNames("ID", "NAME", "AGE", "ROWID")
            .matches(QueryChecker.containsIndexScan("PUBLIC", "PERSON", "_key_PK"))
            .returns(1, "foo1", 19, rowId1)
            .check();
    }

    /** */
    public static @Nullable Object toKeyFromRowId(ExecutionContext<?> ctx, @Nullable String rowIdBase64) {
        if (rowIdBase64 == null)
            return null;

        RowId rowId = RowId.fromBase64String(rowIdBase64);

        // TODO: IGNITE-28223-add-rowid Вот тут сейчас линейный поиск, надо бы переделать, идеи:
        //  1. В ExecutionContext положить IgniteEx - думаю самое простое и лучшее
        //  2. В org.apache.ignite.internal.IgnitionEx.grid(java.util.UUID) сделать map UUID -> IgniteEx - думаю хуже
        IgniteEx n = (IgniteEx) IgnitionEx.grid(ctx.localNodeId());

        CacheObjectContext coctx = n.context().cache().cacheGroup(rowId.cacheGrpId).cacheObjectContext();

        byte[] bs = coctx.restoreIfNecessary(rowId.valBytes);

        try {
            return coctx.unmarshal(bs, null);
        } catch (IgniteCheckedException e) {
            throw new IgniteException(
                String.format("Failed to unmarshal %s: [rowIdBase64=%s]", RowId.class.getSimpleName(), rowIdBase64), e
            );
        }
    }

    /** */
    private static class RowIdPseudoColumnPluginProvider extends AbstractTestPluginProvider {
        /** {@inheritDoc} */
        @Override public String name() {
            return getClass().getSimpleName();
        }

        /** {@inheritDoc} */
        @Override public <T> @Nullable T createComponent(PluginContext ctx, Class<T> cls) {
            if (PseudoColumnProvider.class.equals(cls))
                return (T)(PseudoColumnProvider)() -> List.of(new RowIdPseudoColumn());
            else if (FrameworkConfig.class.equals(cls)) {
                return (T)Frameworks.newConfigBuilder(CalciteQueryProcessor.FRAMEWORK_CONFIG)
                    .operatorTable(SqlOperatorTables.chain(
                        new RowIdPseudoColumnOperatorTable().init(),
                        CalciteQueryProcessor.FRAMEWORK_CONFIG.getOperatorTable()
                    ))
                    .build();
            }

            return super.createComponent(ctx, cls);
        }

        /** {@inheritDoc} */
        @Override public void start(PluginContext ctx) throws IgniteCheckedException {
            RexImpTable.INSTANCE.define(
                RowIdPseudoColumnOperatorTable.TO_KEY_FROM_ROW_ID,
                RexImpTable.createRexCallImplementor((translator, call, translatedOperands) -> {
                    var str = Expressions.convert_(translatedOperands.get(0), String.class);
                    var ectx = Expressions.convert_(translator.getRoot(), ExecutionContext.class);

                    return Expressions.call(RowIdPseudoColumnTest.class, "toKeyFromRowId", ectx, str);
                }, NullPolicy.ANY, false)
            );

            IgniteSqlCallRewriteTable.INSTANCE.register(
                SqlStdOperatorTable.EQUALS.getName(),
                RowIdPseudoColumnTest::rewriteRowIdEquals
            );
        }
    }

    /** */
    private static SqlCall rewriteRowIdEquals(SqlValidator validator, SqlCall call) {
        List<SqlNode> operands = call.getOperandList();

        if (operands.size() != 2)
            return call;

        SqlNode left = operands.get(0);
        SqlNode right = operands.get(1);

        SqlIdentifier rowIdId = rowIdIdentifier(left);
        SqlNode rowIdVal = right;

        if (rowIdId == null) {
            rowIdId = rowIdIdentifier(right);
            rowIdVal = left;
        }

        if (rowIdId == null)
            return call;

        SqlParserPos pos = call.getParserPosition();
        SqlIdentifier keyId = toKeyIdentifier(rowIdId, pos);
        SqlCall toKey = RowIdPseudoColumnOperatorTable.TO_KEY_FROM_ROW_ID.createCall(pos, rowIdVal);

        return new SqlBasicCall(SqlStdOperatorTable.EQUALS, List.of(keyId, toKey), pos);
    }

    /** */
    private static @Nullable SqlIdentifier rowIdIdentifier(SqlNode node) {
        // Validator may wrap identifier with CAST/other unary calls during type coercion.
        // Unwrap a simple CAST chain first to still recognize ROWID.
        SqlNode cur = node;

        while (cur instanceof SqlCall) {
            SqlCall c = (SqlCall)cur;

            if (c.getKind() != SqlKind.CAST || c.operandCount() < 1)
                break;

            cur = c.operand(0);
        }

        if (!(cur instanceof SqlIdentifier))
            return null;

        SqlIdentifier identifier = (SqlIdentifier)cur;

        String lastName = identifier.names.get(identifier.names.size() - 1);

        return RowIdPseudoColumn.COLUMN_NAME.equalsIgnoreCase(lastName) ? identifier : null;
    }

    /** */
    private static SqlIdentifier toKeyIdentifier(SqlIdentifier rowIdId, SqlParserPos pos) {
        if (rowIdId.names.size() == 1)
            return new SqlIdentifier(QueryUtils.KEY_FIELD_NAME, pos);

        return rowIdId.skipLast(1).plus(QueryUtils.KEY_FIELD_NAME, pos);
    }

    /** */
    public static class RowIdPseudoColumnOperatorTable extends ReflectiveSqlOperatorTable {
        /** */
        public static final SqlFunction TO_KEY_FROM_ROW_ID = new SqlFunction(
            "TO_KEY_FROM_ROW_ID",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(SqlTypeName.ANY),
            null,
            OperandTypes.ANY,
            SqlFunctionCategory.USER_DEFINED_FUNCTION
        ) {
            /** {@inheritDoc} */
            @Override public boolean isDeterministic() {
                return false; // важно: чтобы Calcite не константно сворачивал в Java-объект
            }
        };
    }

    /** */
    private static class RowIdPseudoColumn implements PseudoColumnDescriptor {
        /** */
        private static final String COLUMN_NAME = "ROWID";

        /** {@inheritDoc} */
        @Override public String name() {
            return COLUMN_NAME;
        }

        /** {@inheritDoc} */
        @Override public Class<?> type() {
            return String.class;
        }

        /** {@inheritDoc} */
        @Override public int scale() {
            return PseudoColumnDescriptor.NOT_SPECIFIED;
        }

        /** {@inheritDoc} */
        @Override public int precision() {
            return PseudoColumnDescriptor.NOT_SPECIFIED;
        }

        /** {@inheritDoc} */
        @Override public Object value(PseudoColumnValueExtractorContext ctx) throws IgniteCheckedException {
            RowId rowId = RowId.of((PseudoColumnValueExtractorContextEx) ctx);

            return RowId.toBase64String(rowId);
        }
    }

    /** */
    private static class RowId {
        /** */
        @GridToStringInclude
        private final int cacheGrpId;

        /** */
        @GridToStringInclude
        private final byte[] valBytes;

        /** */
        private RowId(int cacheGrpId, byte[] valBytes) {
            this.cacheGrpId = cacheGrpId;
            this.valBytes = valBytes;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(RowId.class, this);
        }

        /** */
        private static RowId of(PseudoColumnValueExtractorContextEx ctx) {
            byte[] valBytes;

            try {
                valBytes = ctx.source().key().valueBytes(ctx.cacheCtx().cacheObjectContext());
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(
                    String.format(
                        "Failed to get valBytes for %s: [cacheName=%s, key=%s]",
                        RowId.class.getSimpleName(), ctx.cacheCtx().name(), ctx.source(true, false)),
                    e
                );
            }

            return new RowId(ctx.cacheCtx().groupId(), valBytes);
        }
        
        /** */
        private static String toBase64String(RowId rowId) {
            byte[] bs;

            try (
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos)
            ) {
                dos.writeInt(rowId.cacheGrpId);
                U.writeByteArray(dos, rowId.valBytes);

                bs = baos.toByteArray();
            } catch (IOException e) {
                throw new IgniteException(
                    String.format("Failed to serialize %s: [value=%s]", RowId.class.getSimpleName(), rowId), e
                );
            }

            return Base64.getEncoder().encodeToString(bs);
        }
        
        /** */
        private static RowId fromBase64String(String s) {
            byte[] decoded = Base64.getDecoder().decode(s);

            try (
                ByteArrayInputStream bais = new ByteArrayInputStream(decoded);
                DataInputStream dis = new DataInputStream(bais)
            ) {
                return new RowId(dis.readInt(), U.readByteArray(dis));
            }
            catch (IOException e) {
                throw new IgniteException(
                    String.format("Failed to deserialize %s: [value=%s]", RowId.class.getSimpleName(), s), e
                );
            }
        }
    }
}
