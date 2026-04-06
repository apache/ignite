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
import org.hamcrest.CoreMatchers;
import org.jetbrains.annotations.Nullable;
import org.junit.Ignore;
import org.junit.Test;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.processors.query.QueryUtils.KEY_FIELD_NAME;
import static org.apache.ignite.internal.processors.query.QueryUtils.PRIMARY_KEY_INDEX;

/**
 * Checks that using {@link QueryUtils#KEY_FIELD_NAME} in condition will use
 * {@link QueryUtils#PRIMARY_KEY_INDEX pk index}.
 */
public class SelectByKeyFieldTest extends AbstractBasicIntegrationTest {
    /** {@inheritDoc} */
    @Override protected int nodeCount() {
        return 1;
    }

    /** */
    @Test
    public void testSimplePk() {
        checkSimplePk(null);
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

    /** */
    @Test
    public void testCompositePk() {
        checkCompositePk(false, true, null);
    }

    /** */
    @Test
    public void testCompositePkSearchByPartOfKey() {
        sql("create table PUBLIC.PERSON(id int, name varchar, surname varchar, age int, primary key(id, name))");

        for (int i = 0; i < 10; i++) {
            sql(
                "insert into PUBLIC.PERSON(id, name, surname, age) values (?, ?, ?, ?)",
                i, "foo" + i, "bar" + i, 18 + i
            );
        }

        List<List<?>> sqlRs = sql("select _key, id, name from PUBLIC.PERSON order by id");
        BinaryObject _key = (BinaryObject)sqlRs.get(6).get(0);
        int id = (Integer)sqlRs.get(6).get(1);
        String name = (String)sqlRs.get(6).get(2);

        assertQuery("select id, name, age, _key from PUBLIC.PERSON where id = ?")
            .withParams(id)
            .matches(QueryChecker.containsIndexScan("PUBLIC", "PERSON", PRIMARY_KEY_INDEX))
            .columnNames("ID", "NAME", "AGE", KEY_FIELD_NAME)
            .returns(id, name, 24, _key)
            .check();

        assertQuery("select id, name, age, _key from PUBLIC.PERSON where name = ?")
            .withParams(name)
            .matches(QueryChecker.containsTableScan("PUBLIC", "PERSON"))
            .columnNames("ID", "NAME", "AGE", KEY_FIELD_NAME)
            .returns(id, name, 24, _key)
            .check();

        assertQuery("select name, age from PUBLIC.PERSON where name = ?")
            .withParams(name)
            .matches(CoreMatchers.not(QueryChecker.containsIndexScan("PUBLIC", "PERSON", PRIMARY_KEY_INDEX)))
            .columnNames("NAME", "AGE")
            .returns(name, 24)
            .check();
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
        checkCompositePk(true, true, null);
    }

    /** */
    @Test
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-28374")
    public void testCompositePkWithKeyTypeAndPersonCompositeKey() {
        checkCompositePk(true, false, null);
    }

    /** */
    @Test
    public void testCompositePkWithOrderByKey() {
        sql("create table PUBLIC.PERSON(id int, name varchar, surname varchar, age int, primary key(id, name))");

        for (int i = 0; i < 10; i++) {
            sql(
                "insert into PUBLIC.PERSON(id, name, surname, age) values (?, ?, ?, ?)",
                i, "foo" + i, "bar" + i, 18 + i
            );
        }

        List<List<?>> sqlRs = sql("select id, name, age, _key from PUBLIC.PERSON");
        sqlRs.sort((o1, o2) -> binaryObjectCmpForDml(o1.get(3), o2.get(3)));

        QueryChecker qryChecker = assertQuery("select id, name, age, _key from PUBLIC.PERSON order by _key")
            .matches(QueryChecker.containsTableScan("PUBLIC", "PERSON"))
            .columnNames("ID", "NAME", "AGE", KEY_FIELD_NAME);

        sqlRs.forEach(objects -> qryChecker.returns(objects.toArray(Object[]::new)));

        qryChecker.check();
    }

    /** */
    @Test
    public void testCompositePkWithDifferentCmpOperations() {
        checkCompositePkWithDifferentCmpOperations(true);
    }

    /** */
    @Test
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-28374")
    public void testCompositePkWithPersonCompositeKeyAndDifferentCmpOperations() {
        checkCompositePkWithDifferentCmpOperations(false);
    }

    /** */
    @Test
    public void testCompositePkWithMinMaxByKey() {
        sql("create table PUBLIC.PERSON(id int, name varchar, surname varchar, age int, primary key(id, name))");

        for (int i = 0; i < 10; i++) {
            sql(
                "insert into PUBLIC.PERSON(id, name, surname, age) values (?, ?, ?, ?)",
                i, "foo" + i, "bar" + i, 18 + i
            );
        }

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
    private void checkSimplePk(@Nullable Runnable executeBeforeChecks) {
        sql("create table PUBLIC.PERSON(id int primary key, name varchar, surname varchar, age int)");

        for (int i = 0; i < 10; i++) {
            sql(
                "insert into PUBLIC.PERSON(id, name, surname, age) values (?, ?, ?, ?)",
                i, "foo" + i, "bar" + i, 18 + i
            );
        }

        List<List<?>> sqlRs = sql("select _key, id from PUBLIC.PERSON order by id");
        int _key = (Integer)sqlRs.get(7).get(0);
        int id = (Integer)sqlRs.get(7).get(1);

        assertEquals(7, _key);
        assertEquals(7, id);

        if (executeBeforeChecks != null)
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
    private void checkCompositePk(
        boolean setKeyTypeToCreateTblDdl,
        boolean useBinaryObject,
        @Nullable Runnable executeBeforeChecks
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

        for (int i = 0; i < 10; i++) {
            sql(
                "insert into PUBLIC.PERSON(id, name, surname, age) values (?, ?, ?, ?)",
                i, "foo" + i, "bar" + i, 18 + i
            );
        }

        List<List<?>> sqlRs = sql("select _key, id, name from PUBLIC.PERSON order by id");
        BinaryObject _key = (BinaryObject)sqlRs.get(6).get(0);
        int id = (Integer)sqlRs.get(6).get(1);
        String name = (String)sqlRs.get(6).get(2);

        assertEquals(6, id);
        assertEquals("foo6", name);

        if (executeBeforeChecks != null)
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
    private void checkCompositePkWithDifferentCmpOperations(boolean useBinaryObject) {
        sql("create table PUBLIC.PERSON(id int, name varchar, surname varchar, age int, primary key(id, name))");

        for (int i = 0; i < 10; i++) {
            sql(
                "insert into PUBLIC.PERSON(id, name, surname, age) values (?, ?, ?, ?)",
                i, "foo" + i, "bar" + i, 18 + i
            );
        }

        List<List<?>> sqlRs = sql("select id, name, age, _key from PUBLIC.PERSON order by id");
        BinaryObjectImpl _key8 = (BinaryObjectImpl)sqlRs.get(8).get(3);

        for (CmpOp cmpOp : CmpOp.values()) {
            if (cmpOp == CmpOp.EQ)
                continue;

            List<List<?>> expRows = sqlRs.stream()
                .filter(objects -> cmpOp.expRowByKeyPred.test((BinaryObjectImpl)objects.get(3), _key8))
                .collect(toList());

            QueryChecker qryChecker = assertQuery(String.format(
                "select id, name, age, _key from PUBLIC.PERSON where _key %s ?", cmpOp.sql
            ))
                .withParams(useBinaryObject ? _key8 : _key8.deserialize())
                .matches(QueryChecker.containsTableScan("PUBLIC", "PERSON"))
                .columnNames("ID", "NAME", "AGE", KEY_FIELD_NAME);

            expRows.forEach(objects -> qryChecker.returns(objects.toArray(Object[]::new)));

            qryChecker.check();
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
        boolean test(BinaryObjectImpl rowKey, BinaryObjectImpl targetKey);
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
        private final String sql;

        /** */
        private final ExpRowByKeyPredicate expRowByKeyPred;

        /** */
        CmpOp(String sql, ExpRowByKeyPredicate expRowByKeyPred) {
            this.sql = sql;
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
