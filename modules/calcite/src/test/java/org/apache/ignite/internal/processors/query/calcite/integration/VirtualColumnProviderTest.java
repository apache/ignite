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
import com.google.common.util.concurrent.AtomicDouble;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.calcite.VirtualColumnDescriptor;
import org.apache.ignite.calcite.VirtualColumnProvider;
import org.apache.ignite.calcite.VirtualColumnValueExtractorContext;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.indexing.IndexingQueryEngineConfiguration;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/** For {@link VirtualColumnProvider} testing. */
// TODO: IGNITE-28223 Добавить больше тестов?
public class VirtualColumnProviderTest extends AbstractBasicIntegrationTest {
    /** */
    private static final String KEY_TO_STRING_COLUMN_NAME = "KEY_TO_STRING";

    /** */
    private static final List<VirtualColumnDescriptor> VIRT_COLS = new CopyOnWriteArrayList<>();

    /** {@inheritDoc} */
    @Override protected int nodeCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        VIRT_COLS.clear();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        VIRT_COLS.clear();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        SqlConfiguration sqlCfg = new SqlConfiguration().setQueryEnginesConfiguration(
            new CalciteQueryEngineConfiguration().setDefault(true),
            new IndexingQueryEngineConfiguration()
        );

        return super.getConfiguration(igniteInstanceName)
            .setSqlConfiguration(sqlCfg)
            .setPluginProviders(new TestVirtualColumnPluginProvider(VIRT_COLS));
    }

    /** */
    @Test
    public void testVirtualColumnWithKeyName() {
        VIRT_COLS.add(new KeyToStingVirtualColumn(QueryUtils.KEY_FIELD_NAME));

        GridTestUtils.assertThrows(
            log,
            () -> sql("create table PUBLIC.PERSON(id int primary key, name varchar)"),
            IgniteSQLException.class,
            "Virtual column name must not match system one: [name=_KEY]"
        );
    }

    /** */
    @Test
    public void testVirtualColumnWithValName() {
        VIRT_COLS.add(new KeyToStingVirtualColumn(QueryUtils.VAL_FIELD_NAME));

        GridTestUtils.assertThrows(
            log,
            () -> sql("create table PUBLIC.PERSON(id int primary key, name varchar)"),
            IgniteSQLException.class,
            "Virtual column name must not match system one: [name=_VAL]"
        );
    }

    /** */
    @Test
    public void testVirtualColumnsWithSameName() {
        VIRT_COLS.add(new KeyToStingVirtualColumn("FOO"));
        VIRT_COLS.add(new KeyToStingVirtualColumn("FOO"));

        GridTestUtils.assertThrows(
            log,
            () -> sql("create table PUBLIC.PERSON(id int primary key, name varchar)"),
            IgniteSQLException.class,
            "Virtual column names must be unique: [name=FOO]"
        );
    }

    /** */
    @Test
    public void testCreateTableWithColumnNameEqualsVirtual() {
        VIRT_COLS.add(new KeyToStingVirtualColumn("NAME"));

        GridTestUtils.assertThrows(
            log,
            () -> sql("create table PUBLIC.PERSON(id int primary key, name varchar)"),
            IgniteSQLException.class,
            "Virtual column name must not overlap with user ones: [name=NAME]"
        );
    }

    /** */
    @Test
    public void testAddVirtualColumn() {
        VIRT_COLS.add(new KeyToStingVirtualColumn(KEY_TO_STRING_COLUMN_NAME));

        sql("create table PUBLIC.PERSON(id int primary key, name varchar)");

        for (int i = 0; i < 2; i++)
            sql("insert into PUBLIC.PERSON(id, name) values (?, ?)", i, "foo" + i);

        // Let's make sure that when using '*' there will be no virtual column.
        assertQuery("select * from PUBLIC.PERSON order by id")
            .columnNames("ID", "NAME")
            .returns(0, "foo0")
            .returns(1, "foo1")
            .check();

        // Let's make sure that when we specify a virtual column, we get it.
        assertQuery(String.format("select id, name, %s from PUBLIC.PERSON order by id", KEY_TO_STRING_COLUMN_NAME))
            .columnNames("ID", "NAME", KEY_TO_STRING_COLUMN_NAME)
            .returns(0, "foo0", "0")
            .returns(1, "foo1", "1")
            .check();

        // Let's check use of a virtual column in where.
        assertQuery(String.format(
            "select id, name, %1$s from PUBLIC.PERSON where %1$s = %2$s order by id",
            KEY_TO_STRING_COLUMN_NAME, "1"
        ))
            .columnNames("ID", "NAME", KEY_TO_STRING_COLUMN_NAME)
            .returns(1, "foo1", "1")
            .check();

        // Let's check use of a virtual column in order by.
        assertQuery(String.format("select id, name, %1$s from PUBLIC.PERSON order by %1$s", KEY_TO_STRING_COLUMN_NAME))
            .columnNames("ID", "NAME", KEY_TO_STRING_COLUMN_NAME)
            .returns(0, "foo0", "0")
            .returns(1, "foo1", "1")
            .check();
    }

    /** */
    @Test
    public void testInsertRowsWithVirtualColumn() {
        VIRT_COLS.add(new KeyToStingVirtualColumn(KEY_TO_STRING_COLUMN_NAME));

        sql("create table PUBLIC.PERSON(id int primary key, name varchar)");

        sql("insert into PUBLIC.PERSON(id, name) values (?, ?)", 0, "foo0");

        assertQuery(String.format("select id, name, %s from PUBLIC.PERSON order by id", KEY_TO_STRING_COLUMN_NAME))
            .columnNames("ID", "NAME", KEY_TO_STRING_COLUMN_NAME)
            .returns(0, "foo0", "0")
            .check();

        // TODO: IGNITE-28223 Проверь что будет с _KEY, _VALUE
        sql(
            String.format("insert into PUBLIC.PERSON(id, name, %s) values (?, ?, ?)", KEY_TO_STRING_COLUMN_NAME),
            1, "foo1", "invalid_value"
        );

        assertQuery(String.format("select id, name, %s from PUBLIC.PERSON order by id", KEY_TO_STRING_COLUMN_NAME))
            .columnNames("ID", "NAME", KEY_TO_STRING_COLUMN_NAME)
            .returns(0, "foo0", "0")
            .returns(0, "foo1", "1")
            .check();
    }

        /** */
    @Test
    public void testUpdateRowsWithVirtualColumn() {
        VIRT_COLS.add(new KeyToStingVirtualColumn(KEY_TO_STRING_COLUMN_NAME));

        sql("create table PUBLIC.PERSON(id int primary key, name varchar)");
        sql("insert into PUBLIC.PERSON(id, name) values (?, ?)", 0, "foo0");

        List<List<?>> sql0 = sql("select id, name, KEY_TO_STRING from PUBLIC.PERSON");

        sql("update PUBLIC.PERSON set name=? where id=?", "foo1", 0);

        List<List<?>> sql1 = sql("select id, name, KEY_TO_STRING from PUBLIC.PERSON");

        sql("update PUBLIC.PERSON set name=?, KEY_TO_STRING=? where id=?", "foo2", "not_valid", 0);

        List<List<?>> sql2 = sql("select id, name, KEY_TO_STRING from PUBLIC.PERSON");

        // TODO: IGNITE-28223 Вот тут почему-то пусто стало...
        assertQuery(String.format("select id, name, %1$s from PUBLIC.PERSON order by %1$s", KEY_TO_STRING_COLUMN_NAME))
            .columnNames("ID", "NAME", KEY_TO_STRING_COLUMN_NAME)
            .returns(0, "foo2", "0")
            .check();
    }

    /** */
    static class TestVirtualColumnPluginProvider extends AbstractTestPluginProvider {
        /** */
        private final List<VirtualColumnDescriptor> virtCols;

        /** */
        TestVirtualColumnPluginProvider(List<VirtualColumnDescriptor> virtCols) {
            this.virtCols = virtCols;
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return getClass().getSimpleName();
        }

        /** {@inheritDoc} */
        @Override public <T> @Nullable T createComponent(PluginContext ctx, Class<T> cls) {
            if (VirtualColumnProvider.class.equals(cls)) {
                return (T)(VirtualColumnProvider)() -> virtCols;
            }

            return super.createComponent(ctx, cls);
        }
    }

    /** */
    static class KeyToStingVirtualColumn implements VirtualColumnDescriptor {
        /** */
        private final String name;

        /** */
        KeyToStingVirtualColumn(String name) {
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
            return VirtualColumnDescriptor.NOT_SPECIFIED;
        }

        /** {@inheritDoc} */
        @Override public int precision() {
            return VirtualColumnDescriptor.NOT_SPECIFIED;
        }

        /** {@inheritDoc} */
        @Override public Object value(VirtualColumnValueExtractorContext ctx) throws IgniteCheckedException {
            return ctx.source(true, true).toString();
        }
    }
}
