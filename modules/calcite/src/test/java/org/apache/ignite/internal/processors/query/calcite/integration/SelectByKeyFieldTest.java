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

import java.util.List;
import java.util.Objects;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.QueryChecker;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.QueryUtils.KEY_FIELD_NAME;
import static org.apache.ignite.internal.processors.query.QueryUtils.PRIMARY_KEY_INDEX;

/**
 * Checks that using {@link QueryUtils#KEY_FIELD_NAME} in condition will use
 * {@link QueryUtils#PRIMARY_KEY_INDEX pk index}.
 */
public class SelectByKeyFieldTest extends AbstractBasicIntegrationTest {
    /** Table size. */
    private static final int TABLE_SIZE = 10;

    /** {@inheritDoc} */
    @Override protected int nodeCount() {
        return 2;
    }

    /** */
    @Test
    public void testSimplePk() {
        checkSimplePk(() -> {});
    }

    /** */
    @Test
    public void testSimplePkAfterAddColumn() {
        checkSimplePk(this::executeAlterTableAddColumn);
    }

    /** */
    @Test
    public void testSimplePkAfterDropColumn() {
        checkSimplePk(this::executeAlterTableDropColumn);
    }

    /**
     * Checks simple primary key search functionality.
     * Creates a table with a simple primary key on {@code id} column, fills it with test data,
     * and verifies that queries using {@code _key} or {@code id} correctly use index scan
     * and return expected results.
     *
     * @param executeBeforeChecks Runnable to execute before performing checks, allowing
     *     for custom setup or validation logic.
     */
    private void checkSimplePk(Runnable executeBeforeChecks) {
        sql("create table PUBLIC.PERSON(id int primary key, name varchar, surname varchar, age int)");

        fillTable();

        List<List<?>> sqlRs = sql("select _key, id from PUBLIC.PERSON order by id");
        int _key = (Integer)sqlRs.get(7).get(0);
        int id = (Integer)sqlRs.get(7).get(1);

        assertEquals(7, _key);
        assertEquals(7, id);

        executeBeforeChecks.run();

        assertQuery("select id, name, age, _key from PUBLIC.PERSON where _key = ?")
            .withParams(_key)
            .matches(QueryChecker.containsIndexScan("PUBLIC", "PERSON", PRIMARY_KEY_INDEX))
            .columnNames("ID", "NAME", "AGE", KEY_FIELD_NAME)
            .returns(id, "foo7", 25, _key)
            .check();

        // Let's check with a smaller number of columns.
        assertQuery("select id, age, _key from PUBLIC.PERSON where _key = ?")
            .withParams(_key)
            .matches(QueryChecker.containsIndexScan("PUBLIC", "PERSON", PRIMARY_KEY_INDEX))
            .columnNames("ID", "AGE", KEY_FIELD_NAME)
            .returns(id, 25, _key)
            .check();

        // Let's just make sure that PK search is not broken.
        assertQuery("select id, name, age, _key from PUBLIC.PERSON where id = ?")
            .withParams(id)
            .matches(QueryChecker.containsIndexScan("PUBLIC", "PERSON", PRIMARY_KEY_INDEX))
            .columnNames("ID", "NAME", "AGE", KEY_FIELD_NAME)
            .returns(id, "foo7", 25, _key)
            .check();
    }

    /** */
    @Test
    public void testCompositePk() {
        checkCompositePk(false, true, () -> {});
    }

    /** */
    @Test
    public void testCompositePkAfterAddColumn() {
        checkCompositePk(false, true, this::executeAlterTableAddColumn);
    }

    /** */
    @Test
    public void testCompositePkAfterDropColumn() {
        checkCompositePk(false, true, this::executeAlterTableDropColumn);
    }

    /** */
    @Test
    public void testCompositePkWithKeyTypeAndBinaryObject() {
        checkCompositePk(true, true, () -> {});
    }

    /** */
    @Test
    public void testCompositePkWithKeyTypeAndPersonCompositeKey() {
        checkCompositePk(true, false, () -> {});
    }

    /** */
    private void checkCompositePk(
        boolean setKeyTypeToCreateTblDdl,
        boolean useBinaryObject,
        Runnable executeBeforeChecks
    ) {
        if (setKeyTypeToCreateTblDdl) {
            // Order of the primary key columns has been deliberately changed.
            sql(String.format(
                "create table PUBLIC.PERSON(id int, name varchar, surname varchar, age int, primary key(name, id)) " +
                    "with \"key_type=%s\"",
                PersonCompositeKey.class.getName()
            ));
        }
        else
            sql("create table PUBLIC.PERSON(id int, name varchar, surname varchar, age int, primary key(id, name))");

        fillTable();

        List<List<?>> sqlRs = sql("select _key, id, name from PUBLIC.PERSON order by id");
        BinaryObject _key = (BinaryObject)sqlRs.get(6).get(0);
        int id = (Integer)sqlRs.get(6).get(1);
        String name = (String)sqlRs.get(6).get(2);

        assertEquals(6, id);
        assertEquals("foo6", name);

        executeBeforeChecks.run();

        assertQuery("select id, name, age, _key from PUBLIC.PERSON where _key = ?")
            .withParams(useBinaryObject ? _key : _key.deserialize())
            .matches(QueryChecker.containsIndexScan("PUBLIC", "PERSON", PRIMARY_KEY_INDEX + "_proxy"))
            .columnNames("ID", "NAME", "AGE", KEY_FIELD_NAME)
            .returns(id, name, 24, _key)
            .check();

        // Let's check with a smaller number of columns.
        assertQuery("select id, age, _key from PUBLIC.PERSON where _key = ?")
            .withParams(useBinaryObject ? _key : _key.deserialize())
            .matches(QueryChecker.containsIndexScan("PUBLIC", "PERSON", PRIMARY_KEY_INDEX + "_proxy"))
            .columnNames("ID", "AGE", KEY_FIELD_NAME)
            .returns(id, 24, _key)
            .check();

        // Let's just make sure that PK search is not broken.
        assertQuery("select id, name, age, _key from PUBLIC.PERSON where id = ? and name = ?")
            .withParams(id, name)
            .matches(QueryChecker.containsIndexScan("PUBLIC", "PERSON", PRIMARY_KEY_INDEX))
            .columnNames("ID", "NAME", "AGE", KEY_FIELD_NAME)
            .returns(id, name, 24, _key)
            .check();
    }

    /** */
    @Test
    public void testCompositePkSearchByPartOfKeyTableScan() {
        compositePkEqualitySearchByPartOfKey(true);
    }

    /** */
    @Test
    public void testCompositePkSearchByPartOfKeyIdxScan() {
        compositePkEqualitySearchByPartOfKey(false);
    }

    /**
     * Tests composite primary key equality search using only part of the key (single column).
     * Verifies that queries with equality comparison on either the {@code id} or {@code name} column
     * return correct results using either table scan or index scan depending on the {@code tableScan} flag.
     *
     * @param tableScan {@code true} to test with table scan, {@code false} to test with index scan.
     */
    public void compositePkEqualitySearchByPartOfKey(boolean tableScan) {
        sql("create table PUBLIC.PERSON(id int, name varchar, surname varchar, age int, primary key(id, name))");

        fillTable();

        List<List<?>> sqlRs = sql("select _key, id, name from PUBLIC.PERSON order by id");
        BinaryObject _key = (BinaryObject)sqlRs.get(6).get(0);
        int id = (Integer)sqlRs.get(6).get(1);
        String name = (String)sqlRs.get(6).get(2);

        // Select by uniq id.
        assertQuery("select /*+ DISABLE_RULE('" + (tableScan ? "LogicalIndexScanConverterRule" : "LogicalTableScanConverterRule") +
                "') */ id, name, age, _key from PUBLIC.PERSON where id = ?")
            .withParams(id)
            .matches(tableScan ?
                QueryChecker.containsTableScan("PUBLIC", "PERSON") :
                QueryChecker.containsIndexScan("PUBLIC", "PERSON", PRIMARY_KEY_INDEX)
            )
            .columnNames("ID", "NAME", "AGE", KEY_FIELD_NAME)
            .returns(id, name, 24, _key)
            .check();

        // Select by uniq name.
        assertQuery("select /*+ DISABLE_RULE('" + (tableScan ? "LogicalIndexScanConverterRule" : "LogicalTableScanConverterRule") +
            "') */ id, name, age, _key from PUBLIC.PERSON where name = ?")
            .withParams(name)
            .matches(tableScan ?
                QueryChecker.containsTableScan("PUBLIC", "PERSON") :
                QueryChecker.containsIndexScan("PUBLIC", "PERSON", PRIMARY_KEY_INDEX)
            )
            .columnNames("ID", "NAME", "AGE", KEY_FIELD_NAME)
            .returns(id, name, 24, _key)
            .check();
    }

    /** */
    @Test
    public void testCompositePkWithOrderByKeyTableScan() {
        compositePkWithOrderByKey(true);
    }

    /** */
    @Test
    public void testCompositePkWithOrderByKeyIdxScan() {
        compositePkWithOrderByKey(false);
    }

    /**
     * Tests composite primary key ordering by {@code _key}.
     * Verifies that ordering by the composite primary key produces correct results
     * when comparing binary objects using {@link #binaryObjectCmpForDml}.
     */
    public void compositePkWithOrderByKey(boolean tableScan) {
        sql("create table PUBLIC.PERSON(id int, name varchar, surname varchar, age int, primary key(id, name))");

        fillTable();

        List<List<?>> sqlRs = sql("select id, name, age, _key from PUBLIC.PERSON");
        sqlRs.sort((o1, o2) -> binaryObjectCmpForDml(o1.get(3), o2.get(3)));

        QueryChecker qryChecker = assertQuery("select /*+ DISABLE_RULE('" +
            (tableScan ? "LogicalIndexScanConverterRule" : "LogicalTableScanConverterRule") +
            "') */ id, name, age, _key from PUBLIC.PERSON order by _key")
            .matches(tableScan ?
                QueryChecker.containsTableScan("PUBLIC", "PERSON") :
                QueryChecker.containsIndexScan("PUBLIC", "PERSON", PRIMARY_KEY_INDEX)
            )
            .columnNames("ID", "NAME", "AGE", KEY_FIELD_NAME);

        sqlRs.forEach(objects -> qryChecker.returns(objects.toArray(Object[]::new)));

        qryChecker.check();
    }

    /** */
    @Test
    public void testBinaryCompositePkComparisonsWithTableScan() {
        checkCompositePkWithDifferentCmpOperations(true, true);
    }

    /** */
    @Test
    public void testBinaryCompositePkComparisonsWithIdxScan() {
        checkCompositePkWithDifferentCmpOperations(true, false);
    }

    /** */
    @Test
    public void testJavaObjCompositePkComparisonsWithTableScan() {
        checkCompositePkWithDifferentCmpOperations(false, true);
    }

    /** */
    @Test
    public void testJavaObjCompositePkComparisonsWithIdxScan() {
        checkCompositePkWithDifferentCmpOperations(false, false);
    }

    /**
     * Checks composite primary key comparisons with different comparison operations.
     * Creates a table with composite key (id, name) using {@link PersonCompositeKey}, inserts test data,
     * and verifies that all comparison operations (EQ, NE, LT, LE, GT, GE) work correctly with both table scan
     * and index scan query strategies.
     *
     * @param useBinaryObject {@code true} to use binary object representation for key comparison,
     *     {@code false} to use deserialized object.
     * @param tableScan {@code true} to test with table scan, {@code false} to test with index scan.
     */
    private void checkCompositePkWithDifferentCmpOperations(boolean useBinaryObject, boolean tableScan) {
        sql(String.format(
            "create table PUBLIC.PERSON(id int, name varchar, surname varchar, age int, primary key(id, name)) with \"key_type=%s\"",
            PersonCompositeKey.class.getName()
        ));

        fillTable();

        List<List<?>> sqlRs = sql("select id, name, age, _key from PUBLIC.PERSON order by id");

        Object key8 = sqlRs.get(8).get(3);

        for (CmpOp cmpOp : CmpOp.values()) {
            List<List<?>> expRows = sqlRs.stream()
                .filter(objects -> cmpOp.expRowByKeyPred.test((BinaryObjectImpl)objects.get(3), key8))
                .toList();

            QueryChecker qryChecker = assertQuery(String.format(
                "select /*+ DISABLE_RULE('" + (tableScan ? "LogicalIndexScanConverterRule" : "LogicalTableScanConverterRule") +
                    "') */ id, name, age, _key from PUBLIC.PERSON where _key %s ?", cmpOp.comp
            ))
                .withParams(useBinaryObject ? key8 : ((BinaryObject)key8).deserialize())
                .matches(tableScan ?
                    QueryChecker.containsTableScan("PUBLIC", "PERSON") :
                    QueryChecker.containsIndexScan("PUBLIC", "PERSON", PRIMARY_KEY_INDEX)
                )
                .columnNames("ID", "NAME", "AGE", KEY_FIELD_NAME);

            expRows.forEach(objects -> qryChecker.returns(objects.toArray(Object[]::new)));

            qryChecker.check();
        }
    }

    /** */
    @Test
    public void testResultsWithUnexpectedParam() {
        sql(
            "create table PUBLIC.PERSON(id int, name varchar, surname varchar, age int, primary key(id, name)) with \"key_type=%s\""
                .formatted("UndefinedClassName")
        );

        checkResultsWithUnexpectedKey(new Object());
    }

    /** */
    @Test
    public void testResultsWithoutKeyTypeWithUnexpectedParam() {
        sql("create table PUBLIC.PERSON(id int, name varchar, surname varchar, age int, primary key(id, name))");

        checkResultsWithUnexpectedKey(new Object());
    }

    /** */
    @Test
    public void testResultsWithoutKeyTypeWithUnexpectedBOAsParam() {
        sql("create table PUBLIC.PERSON(id int, name varchar, surname varchar, age int, primary key(id, name))");

        Object arg = client.binary().builder("UserDefinedBinary").build();

        checkResultsWithUnexpectedKey(arg);
    }

    /** Check results with search for _key different from defined in schema. */
    private void checkResultsWithUnexpectedKey(Object arg) {
        String qryTemplate = "select /*+ DISABLE_RULE('%s') */ _key from PUBLIC.PERSON where _key %s ? ORDER BY _key";

        fillTable();

        for (CmpOp cmpOp : CmpOp.values()) {
            List<List<?>> res = null;

            for (boolean tableScan : List.of(true, false)) {
                String qry = qryTemplate.formatted(tableScan ?
                    "LogicalIndexScanConverterRule" : "LogicalTableScanConverterRule", cmpOp.comp);

                if (res == null)
                    res = sql(qry, arg);
                else {
                    List<List<?>> secondRes = sql(qry, arg);

                    assertEquals("Different results size", res.size(), secondRes.size());

                    for (int pos = 0; pos < secondRes.size(); pos++)
                        assertEquals(res.get(pos).get(0), secondRes.get(pos).get(0));
                }

                assertQuery(qry)
                    .withParams(arg)
                    .matches(tableScan ?
                        QueryChecker.containsTableScan("PUBLIC", "PERSON") :
                        QueryChecker.containsIndexScan("PUBLIC", "PERSON", PRIMARY_KEY_INDEX)
                    )
                    .check();
            }
        }
    }

    /** */
    @Test
    public void testCompositePkWithMinMaxByKey() {
        sql("create table PUBLIC.PERSON(id int, name varchar, surname varchar, age int, primary key(id, name))");

        fillTable();

        List<List<?>> sqlRs = sql("select _key from PUBLIC.PERSON order by id");
        List<?> min = sqlRs.stream().min((o1, o2) -> binaryObjectCmpForDml(o1.get(0), o2.get(0))).orElseThrow();
        List<?> max = sqlRs.stream().max((o1, o2) -> binaryObjectCmpForDml(o1.get(0), o2.get(0))).orElseThrow();

        assertQuery("select min(_key) from PUBLIC.PERSON")
            .matches(QueryChecker.containsTableScan("PUBLIC", "PERSON"))
            .columnNames("MIN(_KEY)")
            .returns(min.get(0))
            .check();

        assertQuery("select max(_key) from PUBLIC.PERSON")
            .matches(QueryChecker.containsTableScan("PUBLIC", "PERSON"))
            .columnNames("MAX(_KEY)")
            .returns(max.get(0))
            .check();
    }

    /** */
    private void fillTable() {
        for (int i = 0; i < TABLE_SIZE; i++) {
            sql(
                "insert into PUBLIC.PERSON(id, name, surname, age) values (?, ?, ?, ?)",
                i, "foo" + i, "bar" + i, 18 + i
            );
        }
    }

    /** */
    public static class PersonCompositeKey {
        /** */
        @GridToStringInclude
        public int id;

        /** */
        @GridToStringInclude
        public String name;

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(id, name);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            if (obj == this)
                return true;

            if (!(obj instanceof PersonCompositeKey))
                return false;

            PersonCompositeKey that = (PersonCompositeKey)obj;

            return id == that.id && name.equals(that.name);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(PersonCompositeKey.class, this);
        }
    }

    /** */
    @FunctionalInterface
    private interface ExpRowByKeyPredicate {
        /** */
        boolean test(BinaryObjectImpl rowKey, Object targetKey);
    }

    /** */
    private enum CmpOp {
        /** */
        GE(">=", (rowKey, targetKey) -> binaryObjectCmpForDml(rowKey, targetKey) >= 0),

        /** */
        GT(">", (rowKey, targetKey) -> binaryObjectCmpForDml(rowKey, targetKey) > 0),

        /** */
        LE("<=", (rowKey, targetKey) -> binaryObjectCmpForDml(rowKey, targetKey) <= 0),

        /** */
        LT("<", (rowKey, targetKey) -> binaryObjectCmpForDml(rowKey, targetKey) < 0),

        /** */
        EQ("=", (rowKey, targetKey) -> binaryObjectCmpForDml(rowKey, targetKey) == 0),

        /** */
        NE("<>", (rowKey, targetKey) -> binaryObjectCmpForDml(rowKey, targetKey) != 0);

        /** */
        private final String comp;

        /** */
        private final ExpRowByKeyPredicate expRowByKeyPred;

        /** */
        CmpOp(String comp, ExpRowByKeyPredicate expRowByKeyPred) {
            this.comp = comp;
            this.expRowByKeyPred = expRowByKeyPred;
        }
    }

    /** */
    private void executeAlterTableAddColumn() {
        sql("alter table PUBLIC.PERSON add email varchar");
    }

    /** */
    private void executeAlterTableDropColumn() {
        sql("alter table PUBLIC.PERSON drop column surname");
    }

    /** */
    private static int binaryObjectCmpForDml(Object o1, Object o2) {
        return BinaryUtils.binariesFactory.compareForDml(o1, o2);
    }
}
