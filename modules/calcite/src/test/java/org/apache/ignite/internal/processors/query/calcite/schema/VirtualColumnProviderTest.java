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

import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.indexing.IndexingQueryEngineConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.integration.AbstractBasicIntegrationTest;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/** For {@link VirtualColumnProvider} testing. */
public class VirtualColumnProviderTest extends AbstractBasicIntegrationTest {
    /** */
    private static final String KEY_TO_STRING_COLUMN_NAME = "KEY_TO_STRING";

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
            .setPluginProviders(new TestVirtualColumnPluginProvider());
    }

    /** */
    @Test
    public void test() {
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

        // Let's check the use of a virtual column in where.
        assertQuery(String.format(
            "select id, name, %1$s from PUBLIC.PERSON where %1$s = %2$s order by id",
            KEY_TO_STRING_COLUMN_NAME, "1"
        ))
            .columnNames("ID", "NAME", KEY_TO_STRING_COLUMN_NAME)
            .returns(1, "foo1", "1")
            .check();

        // Let's check the use of a virtual column in order by.
        assertQuery(String.format("select id, name, %1$s from PUBLIC.PERSON order by %1$s", KEY_TO_STRING_COLUMN_NAME))
            .columnNames("ID", "NAME", KEY_TO_STRING_COLUMN_NAME)
            .returns(0, "foo0", "0")
            .returns(1, "foo1", "1")
            .check();
    }

    /** */
    private static class TestVirtualColumnPluginProvider extends AbstractTestPluginProvider {
        /** {@inheritDoc} */
        @Override public String name() {
            return getClass().getSimpleName();
        }

        /** {@inheritDoc} */
        @Override public <T> @Nullable T createComponent(PluginContext ctx, Class<T> cls) {
            if (VirtualColumnProvider.class.equals(cls)) {
                return (T) (VirtualColumnProvider) nxtColIdx -> List.of(new KeyToStingVirtualColumn(nxtColIdx));
            }

            return super.createComponent(ctx, cls);
        }
    }

    /** */
    private static class KeyToStingVirtualColumn extends AbstractTestCacheColumnDescriptor {
        /** */
        private KeyToStingVirtualColumn(int idx) {
            super(idx, KEY_TO_STRING_COLUMN_NAME, Type.nullable(String.class), false, false);
        }

        /** {@inheritDoc} */
        @Override public Object value(
            ExecutionContext<?> ectx,
            GridCacheContext<?, ?> cctx,
            CacheDataRow src
        ) throws IgniteCheckedException {
            return cctx.unwrapBinaryIfNeeded(src.key(), false, null).toString();
        }

        /** {@inheritDoc} */
        @Override public void set(Object dst, Object val) {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public boolean hasDefaultValue() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public Object defaultValue() {
            throw new UnsupportedOperationException();
        }
    }
}
