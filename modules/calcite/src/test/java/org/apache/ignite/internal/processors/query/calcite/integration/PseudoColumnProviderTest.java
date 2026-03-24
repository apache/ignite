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
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.calcite.PseudoColumnDescriptor;
import org.apache.ignite.calcite.PseudoColumnProvider;
import org.apache.ignite.calcite.PseudoColumnValueExtractorContext;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.indexing.IndexingQueryEngineConfiguration;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/** For {@link PseudoColumnProvider} testing. */
public class PseudoColumnProviderTest extends AbstractBasicIntegrationTest {
    /** */
    private static final String KEY_TO_STRING_COLUMN_NAME = "KEY_TO_STRING";

    /** */
    private static final List<PseudoColumnDescriptor> PSEUDO_COLS = new CopyOnWriteArrayList<>();

    /** {@inheritDoc} */
    @Override protected int nodeCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        PSEUDO_COLS.clear();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        PSEUDO_COLS.clear();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        SqlConfiguration sqlCfg = new SqlConfiguration().setQueryEnginesConfiguration(
            new CalciteQueryEngineConfiguration().setDefault(true),
            new IndexingQueryEngineConfiguration()
        );

        return super.getConfiguration(igniteInstanceName)
            .setSqlConfiguration(sqlCfg)
            .setPluginProviders(new TestPseudoColumnPluginProvider(PSEUDO_COLS));
    }

    /** */
    @Test
    public void testCreateTableWithColumnNameEqualsPseudo() {
        PSEUDO_COLS.add(new KeyToStingPseudoColumn("NAME"));

        assertThrows(
            "create table PUBLIC.PERSON(id int primary key, name varchar)",
            IgniteSQLException.class,
            "Pseudocolumn name must not overlap with user ones: [name=NAME]"
        );
    }

    /** */
    @Test
    public void testInsertRowsWithPseudoColumn() {
        PSEUDO_COLS.add(new KeyToStingPseudoColumn(KEY_TO_STRING_COLUMN_NAME));

        sql("create table PUBLIC.PERSON(id int primary key, name varchar)");

        sql("insert into PUBLIC.PERSON(id, name) values (?, ?)", 0, "foo0");

        assertQuery(String.format("select id, name, %s from PUBLIC.PERSON order by id", KEY_TO_STRING_COLUMN_NAME))
            .columnNames("ID", "NAME", KEY_TO_STRING_COLUMN_NAME)
            .returns(0, "foo0", "0")
            .check();

        assertThrows(
            String.format("insert into PUBLIC.PERSON(id, name, %s) values (?, ?, ?)", KEY_TO_STRING_COLUMN_NAME),
            IgniteSQLException.class,
            String.format("Cannot insert field \"%s\". You cannot insert pseudocolumn.", KEY_TO_STRING_COLUMN_NAME),
            1, "foo1", "1"
        );

        assertQuery(String.format("select id, name, %s from PUBLIC.PERSON order by id", KEY_TO_STRING_COLUMN_NAME))
            .columnNames("ID", "NAME", KEY_TO_STRING_COLUMN_NAME)
            .returns(0, "foo0", "0")
            .check();
    }

        /** */
    @Test
    public void testUpdateRowsWithPseudoColumn() {
        PSEUDO_COLS.add(new KeyToStingPseudoColumn(KEY_TO_STRING_COLUMN_NAME));

        sql("create table PUBLIC.PERSON(id int primary key, name varchar)");

        sql("insert into PUBLIC.PERSON(id, name) values (?, ?)", 0, "foo0");

        sql("update PUBLIC.PERSON set name=? where id=?", "foo1", 0);

        assertQuery(String.format("select id, name, %s from PUBLIC.PERSON order by id", KEY_TO_STRING_COLUMN_NAME))
            .columnNames("ID", "NAME", KEY_TO_STRING_COLUMN_NAME)
            .returns(0, "foo1", "0")
            .check();

        assertThrows(
            String.format("update PUBLIC.PERSON set name=?, %s=? where id=?", KEY_TO_STRING_COLUMN_NAME),
            IgniteSQLException.class,
            String.format("Cannot update field \"%s\". You cannot update pseudocolumn.", KEY_TO_STRING_COLUMN_NAME),
            "foo2", "2", 0
        );

        assertQuery(String.format("select id, name, %1$s from PUBLIC.PERSON order by %1$s", KEY_TO_STRING_COLUMN_NAME))
            .columnNames("ID", "NAME", KEY_TO_STRING_COLUMN_NAME)
            .returns(0, "foo1", "0")
            .check();
    }

    /** */
    @Test
    public void testSelectPseudoColumn() {
        PSEUDO_COLS.add(new KeyToStingPseudoColumn(KEY_TO_STRING_COLUMN_NAME));

        sql("create table PUBLIC.PERSON(id int primary key, name varchar)");

        for (int i = 0; i < 2; i++)
            sql("insert into PUBLIC.PERSON(id, name) values (?, ?)", i, "foo" + i);

        // Let's make sure that when using '*' there will be no pseudocolumn.
        assertQuery("select * from PUBLIC.PERSON order by id")
            .columnNames("ID", "NAME")
            .returns(0, "foo0")
            .returns(1, "foo1")
            .check();

        // Let's make sure that when we specify a pseudocolumn, we get it.
        assertQuery(String.format("select id, name, %s from PUBLIC.PERSON order by id", KEY_TO_STRING_COLUMN_NAME))
            .columnNames("ID", "NAME", KEY_TO_STRING_COLUMN_NAME)
            .returns(0, "foo0", "0")
            .returns(1, "foo1", "1")
            .check();

        // Let's check use of a pseudocolumn in where.
        assertQuery(String.format(
            "select id, name, %1$s from PUBLIC.PERSON where %1$s = %2$s order by id",
            KEY_TO_STRING_COLUMN_NAME, "1"
        ))
            .columnNames("ID", "NAME", KEY_TO_STRING_COLUMN_NAME)
            .returns(1, "foo1", "1")
            .check();

        // Let's check use of a pseudocolumn in order by.
        assertQuery(String.format("select id, name, %1$s from PUBLIC.PERSON order by %1$s", KEY_TO_STRING_COLUMN_NAME))
            .columnNames("ID", "NAME", KEY_TO_STRING_COLUMN_NAME)
            .returns(0, "foo0", "0")
            .returns(1, "foo1", "1")
            .check();
    }

    /** */
    static class TestPseudoColumnPluginProvider extends AbstractTestPluginProvider {
        /** */
        private final List<PseudoColumnDescriptor> cols;

        /** */
        TestPseudoColumnPluginProvider(List<PseudoColumnDescriptor> cols) {
            this.cols = cols;
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return getClass().getSimpleName();
        }

        /** {@inheritDoc} */
        @Override public <T> @Nullable T createComponent(PluginContext ctx, Class<T> cls) {
            if (PseudoColumnProvider.class.equals(cls)) {
                return (T)(PseudoColumnProvider)() -> cols;
            }

            return super.createComponent(ctx, cls);
        }
    }

    /** */
    static class KeyToStingPseudoColumn implements PseudoColumnDescriptor {
        /** */
        private final String name;

        /** */
        KeyToStingPseudoColumn(String name) {
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return name;
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
            return ctx.source(true, true).toString();
        }
    }
}
