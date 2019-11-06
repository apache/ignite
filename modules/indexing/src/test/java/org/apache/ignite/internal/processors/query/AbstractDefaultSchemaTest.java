/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/** Abstract test to verify default sql schema. */
public abstract class AbstractDefaultSchemaTest extends AbstractIndexingCommonTest {
    /** Table name. */
    private static final String TBL_NAME = "T1";

    /**
     * @param qry Query.
     */
    protected abstract List<List<?>> execSql(String qry);

    /**
     * @param withSchema Whether to specify schema or not.
     */
    public String tableName(boolean withSchema) {
        String prefix = "";

        if (withSchema)
            prefix += "PUBLIC.";

        return prefix + TBL_NAME;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(0);
    }

    /** */
    @Test
    public void testBasicOpsExplicitPublicSchema() {
        executeStmtsAndVerify(() -> true);
    }

    /** */
    @Test
    public void testBasicOpsImplicitPublicSchema() {
        executeStmtsAndVerify(() -> false);
    }

    /** */
    @Test
    public void testBasicOpsMixedPublicSchema() {
        AtomicInteger i = new AtomicInteger();

        executeStmtsAndVerify(() -> (i.incrementAndGet() & 1) == 0);
    }

    /** */
    @Test
    @SuppressWarnings("ThrowableNotThrown")
    public void testCreateDropNonExistingSchema() {
        GridTestUtils.assertThrowsWithCause(
            () -> sql("CREATE TABLE UNKNOWN_SCHEMA." + TBL_NAME + "(id INT PRIMARY KEY, val INT)"),
            SQLException.class
        );

        GridTestUtils.assertThrowsWithCause(
            () -> sql("DROP TABLE UNKNOWN_SCHEMA." + TBL_NAME),
            SQLException.class
        );
    }

    /** */
    @Ignore("https://ggsystems.atlassian.net/browse/GG-25387")
    @Test
    public void testDropIfExistsNonExistingSchema() {
        sql("DROP TABLE IF EXISTS UNKNOWN_SCHEMA." + TBL_NAME);
    }

    /** */
    private void executeStmtsAndVerify(Supplier<Boolean> withSchemaDecisionSup) {
        sql("CREATE TABLE " + tableName(withSchemaDecisionSup.get()) + " (id INT PRIMARY KEY, val INT)");

        sql("CREATE INDEX t1_idx_1 ON " + tableName(withSchemaDecisionSup.get()) + "(val)");

        sql("INSERT INTO " + tableName(withSchemaDecisionSup.get()) + " (id, val) VALUES(1, 2)");
        sql("SELECT * FROM " + tableName(withSchemaDecisionSup.get()), res -> oneRowList(1, 2).equals(res));

        sql("UPDATE " + tableName(withSchemaDecisionSup.get()) + " SET val = 5");
        sql("SELECT * FROM " + tableName(withSchemaDecisionSup.get()), res -> oneRowList(1, 5).equals(res));

        sql("DELETE FROM " + tableName(withSchemaDecisionSup.get()) + " WHERE id = 1");
        sql("SELECT COUNT(*) FROM " + tableName(withSchemaDecisionSup.get()), res -> oneRowList(0L).equals(res));

        sql("SELECT COUNT(*) FROM " + QueryUtils.SCHEMA_SYS + ".TABLES WHERE schema_name = 'PUBLIC' " +
            "AND table_name = \'" + TBL_NAME + "\'", res -> oneRowList(1L).equals(res));

        sql("DROP TABLE " + tableName(withSchemaDecisionSup.get()));
    }

    /** */
    private List<List<?>> oneRowList(Object... args) {
        return Collections.singletonList(Arrays.asList(args));
    }

    /** */
    protected void sql(String qry) {
        sql(qry, null);
    }

    /** */
    protected void sql(String qry, @Nullable Predicate<List<List<?>>> validator) {
        List<List<?>> res = execSql(qry);

        if (validator != null)
            Assert.assertTrue(validator.test(res));
    }
}
