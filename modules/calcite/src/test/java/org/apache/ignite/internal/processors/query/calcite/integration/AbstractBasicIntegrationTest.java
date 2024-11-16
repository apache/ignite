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
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.QueryContext;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.QueryChecker;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionServiceImpl;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.RangeIterable;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;
import org.apache.ignite.internal.processors.query.calcite.prepare.bounds.SearchBounds;
import org.apache.ignite.internal.processors.query.calcite.rel.logical.IgniteLogicalIndexScan;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteIndex;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.query.calcite.exec.ExchangeServiceImpl.INBOX_INITIALIZATION_TIMEOUT;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 *
 */
public class AbstractBasicIntegrationTest extends GridCommonAbstractTest {
    /** */
    protected static final Object[] NULL_RESULT = new Object[] { null };

    /** */
    protected static final String TABLE_NAME = "person";

    /** */
    protected static IgniteEx client;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(nodeCount());

        client = startClientGrid("client");
    }

    /** */
    protected boolean destroyCachesAfterTest() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        // Wait for pending queries before destroying caches. If some error occurs during query execution, client code
        // can get control earlier than query leave the running queries registry (need some time for async message
        // exchange), but eventually, all queries should be closed.
        assertTrue("Not finished queries found on client", waitForCondition(
            () -> queryProcessor(client).queryRegistry().runningQueries().isEmpty(), 1_000L));

        waitForCondition(() -> {
            for (Ignite ign : G.allGrids()) {
                if (!queryProcessor(ign).mailboxRegistry().inboxes().isEmpty())
                    return false;
            }

            return true;
        }, INBOX_INITIALIZATION_TIMEOUT * 2);

        for (Ignite ign : G.allGrids()) {
            if (destroyCachesAfterTest()) {
                for (String cacheName : ign.cacheNames())
                    ign.destroyCache(cacheName);
            }

            CalciteQueryProcessor qryProc = queryProcessor(ign);

            assertEquals("Not finished queries found [ignite=" + ign.name() + ']',
                0, qryProc.queryRegistry().runningQueries().size());

            ExecutionServiceImpl<Object[]> execSvc = (ExecutionServiceImpl<Object[]>)qryProc.executionService();
            assertEquals("Tracked memory must be 0 after test [ignite=" + ign.name() + ']',
                0, execSvc.memoryTracker().allocated());

            assertEquals("Count of inboxes must be 0 after test [ignite=" + ign.name() + ']',
                0, qryProc.mailboxRegistry().inboxes().size());

            assertEquals("Count of outboxes must be 0 after test [ignite=" + ign.name() + ']',
                0, qryProc.mailboxRegistry().outboxes().size());
        }

        awaitPartitionMapExchange();

        cleanQueryPlanCache();
    }

    /** */
    protected int nodeCount() {
        return 3;
    }

    /** */
    protected void cleanQueryPlanCache() {
        for (Ignite ign : G.allGrids()) {
            CalciteQueryProcessor qryProc = (CalciteQueryProcessor)Commons.lookupComponent(
                ((IgniteEx)ign).context(), QueryEngine.class);

            qryProc.queryPlanCache().clear();
        }
    }

    /** */
    protected QueryChecker assertQuery(String qry) {
        return assertQuery(client, qry);
    }

    /** */
    protected QueryChecker assertQuery(IgniteEx ignite, String qry) {
        return new QueryChecker(qry) {
            @Override protected QueryEngine getEngine() {
                return Commons.lookupComponent(((IgniteEx)ignite).context(), QueryEngine.class);
            }
        };
    }

    /** @deprecated Use {@link #sql(String, Object...)} instead. */
    @Deprecated
    protected List<List<?>> executeSql(String sql, Object... args) {
        return executeSql(client, sql, args);
    }

    /** @deprecated Use {@link #sql(String, Object...)} instead. */
    @Deprecated
    protected List<List<?>> executeSql(IgniteEx ignite, String sql, Object... args) {
        CalciteQueryProcessor qryProc = Commons.lookupComponent(ignite.context(), CalciteQueryProcessor.class);

        List<FieldsQueryCursor<List<?>>> cur = qryProc.query(queryContext(), "PUBLIC", sql, args);

        try (QueryCursor<List<?>> srvCursor = cur.get(0)) {
            return srvCursor.getAll();
        }
    }

    /** */
    protected QueryContext queryContext() {
        return null;
    }

    /**
     * Asserts that executeSql throws an exception.
     *
     * @param sql Query.
     * @param cls Exception class.
     * @param msg Error message.
     */
    protected void assertThrows(String sql, Class<? extends Exception> cls, String msg, Object... args) {
        assertThrowsAnyCause(log, () -> sql(sql, args), cls, msg);
    }

    /** */
    protected IgniteCache<Integer, Employer> createAndPopulateTable() {
        return createAndPopulateTable(client, 2, CacheMode.PARTITIONED);
    }

    /** */
    protected IgniteCache<Integer, Employer> createAndPopulateTable(Ignite ignite, int backups, CacheMode cacheMode) {
        IgniteCache<Integer, Employer> person = ignite.getOrCreateCache(this.<Integer, Employer>cacheConfiguration()
            .setName(TABLE_NAME)
            .setSqlSchema("PUBLIC")
            .setQueryEntities(F.asList(new QueryEntity(Integer.class, Employer.class)
                .setTableName(TABLE_NAME)
                .addQueryField("ID", Integer.class.getName(), null)
                .setKeyFieldName("ID")
            ))
            .setCacheMode(cacheMode)
            .setBackups(backups)
        );

        int idx = 0;

        put(ignite, person, idx++, new Employer("Igor", 10d));
        put(ignite, person, idx++, new Employer(null, 15d));
        put(ignite, person, idx++, new Employer("Ilya", 15d));
        put(ignite, person, idx++, new Employer("Roma", 10d));
        put(ignite, person, idx, new Employer("Roma", 10d));

        return person;
    }

    /** */
    protected <K, V> void put(Ignite ignite, IgniteCache<K, V> c, K key, V val) {
        c.put(key, val);
    }

    /** */
    protected <K, V> CacheConfiguration<K, V> cacheConfiguration() {
        return new CacheConfiguration<>();
    }

    /** */
    protected CalciteQueryProcessor queryProcessor(Ignite ignite) {
        return Commons.lookupComponent(((IgniteEx)ignite).context(), CalciteQueryProcessor.class);
    }

    /** */
    protected List<List<?>> sql(String sql, Object... params) {
        return sql(client, sql, params);
    }

    /** */
    protected List<List<?>> sql(IgniteEx ignite, String sql, Object... params) {
        List<FieldsQueryCursor<List<?>>> cur = queryProcessor(ignite).query(queryContext(), "PUBLIC", sql, params);

        try (QueryCursor<List<?>> srvCursor = cur.get(0)) {
            return srvCursor.getAll();
        }
    }

    /** */
    public static class DelegatingIgniteIndex implements IgniteIndex {
        /** */
        protected final IgniteIndex delegate;

        /** */
        public DelegatingIgniteIndex(IgniteIndex delegate) {
            this.delegate = delegate;
        }

        /** {@inheritDoc} */
        @Override public RelCollation collation() {
            return delegate.collation();
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return delegate.name();
        }

        /** {@inheritDoc} */
        @Override public IgniteTable table() {
            return delegate.table();
        }

        /** {@inheritDoc} */
        @Override public IgniteLogicalIndexScan toRel(
            RelOptCluster cluster,
            RelOptTable relOptTbl,
            @Nullable List<RexNode> proj,
            @Nullable RexNode cond,
            @Nullable ImmutableBitSet requiredColumns
        ) {
            return delegate.toRel(cluster, relOptTbl, proj, cond, requiredColumns);
        }

        /** {@inheritDoc} */
        @Override public List<SearchBounds> toSearchBounds(
            RelOptCluster cluster,
            @Nullable RexNode cond,
            @Nullable ImmutableBitSet requiredColumns
        ) {
            return delegate.toSearchBounds(cluster, cond, requiredColumns);
        }

        /** {@inheritDoc} */
        @Override public <Row> Iterable<Row> scan(
            ExecutionContext<Row> execCtx,
            ColocationGroup grp,
            RangeIterable<Row> ranges,
            @Nullable ImmutableBitSet requiredColumns
        ) {
            return delegate.scan(execCtx, grp, ranges, requiredColumns);
        }

        /** {@inheritDoc} */
        @Override public long count(ExecutionContext<?> ectx, ColocationGroup grp, boolean notNull) {
            return delegate.count(ectx, grp, notNull);
        }

        /** {@inheritDoc} */
        @Override public <Row> Iterable<Row> firstOrLast(
            boolean first,
            ExecutionContext<Row> ectx,
            ColocationGroup grp,
            @Nullable ImmutableBitSet requiredColumns
        ) {
            return delegate.firstOrLast(first, ectx, grp, requiredColumns);
        }

        /** {@inheritDoc} */
        @Override public boolean isInlineScanPossible(@Nullable ImmutableBitSet requiredColumns) {
            return delegate.isInlineScanPossible(requiredColumns);
        }
    }

    /** */
    public static class Employer {
        /** */
        @QuerySqlField
        public String name;

        /** */
        @QuerySqlField
        public Double salary;

        /** */
        public Employer(String name, Double salary) {
            this.name = name;
            this.salary = salary;
        }
    }
}
