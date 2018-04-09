/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.cache;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;

import org.hamcrest.Matchers;
import org.jetbrains.annotations.NotNull;
import org.junit.Ignore;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;

/**
 *
 */
public class IgniteDynamicSqlRestoreTest extends GridCommonAbstractTest implements Serializable {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setAutoActivationEnabled(false);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setMaxSize(200 * 1024 * 1024).setPersistenceEnabled(true))
            .setWalMode(WALMode.LOG_ONLY);

        cfg.setDataStorageConfiguration(memCfg);

        cfg.setConsistentId(gridName);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception if failed.
     */
    public void testMergeChangedConfigOnCoordinator() throws Exception {
        {
            //given: two started nodes with test table
            Ignite ig = startGrid(0);
            startGrid(1);

            ig.cluster().active(true);

            IgniteCache cache = ig.getOrCreateCache(getTestTableConfiguration());

            fillTestData(ig);

            //when: stop one node and create indexes on other node
            stopGrid(1);

            cache.query(new SqlFieldsQuery("create index myindexa on TestIndexObject(a)")).getAll();
            cache.query(new SqlFieldsQuery("create index myindexb on TestIndexObject(b)")).getAll();
            cache.query(new SqlFieldsQuery("alter table TestIndexObject add column (c int)")).getAll();

            //and: stop all grid
            stopAllGrids();
        }

        {
            //and: start cluster from node without index
            IgniteEx ig = startGrid(1);
            startGrid(0);

            ig.cluster().active(true);

            //and: change data
            try (IgniteDataStreamer<Object, Object> s = ig.dataStreamer("test")) {
                s.allowOverwrite(true);
                for (int i = 0; i < 5_000; i++)
                    s.addData(i, null);
            }

            stopAllGrids();
        }

        {
            //when: start node from first node
            IgniteEx ig = startGrid(0);
            startGrid(1);

            ig.cluster().active(true);

            //then: everything is ok
            try (IgniteDataStreamer<Object, Object> s = ig.dataStreamer("test")) {
                s.allowOverwrite(true);
                for (int i = 0; i < 50_000; i++) {
                    BinaryObject bo = ig.binary().builder("TestIndexObject")
                        .setField("a", i, Object.class)
                        .setField("b", String.valueOf(i), Object.class)
                        .setField("c", i, Object.class)
                        .build();

                    s.addData(i, bo);
                }
            }

            IgniteCache<Object, Object> cache = ig.getOrCreateCache("test");

            assertThat(doExplainPlan(cache, "explain select * from TestIndexObject where a > 5"), containsString("myindexa"));
            assertThat(cache.query(new SqlFieldsQuery("SELECT a,b,c FROM TestIndexObject limit 1")).getAll(), is(not(empty())));
        }
    }

    /**
     * @throws Exception if failed.
     */
    public void testTakeConfigFromJoiningNodeOnInactiveGrid() throws Exception {
        {
            //given: two started nodes with test table
            Ignite ig = startGrid(0);
            startGrid(1);

            ig.cluster().active(true);

            stopGrid(1);

            IgniteCache cache = ig.getOrCreateCache(getTestTableConfiguration());

            cache.query(new SqlFieldsQuery("create index myindexa on TestIndexObject(a)")).getAll();
            cache.query(new SqlFieldsQuery("create index myindexb on TestIndexObject(b)")).getAll();
            cache.query(new SqlFieldsQuery("alter table TestIndexObject add column (c int)")).getAll();

            //and: stop all grid
            stopAllGrids();
        }

        {
            //and: start cluster from node without cache
            IgniteEx ig = startGrid(1);
            startGrid(0);

            ig.cluster().active(true);

            //then: config for cache was applying successful
            fillTestData(ig);

            IgniteCache<Object, Object> cache = ig.getOrCreateCache("test");

            assertThat(doExplainPlan(cache, "explain select * from TestIndexObject where a > 5"), containsString("myindexa"));
            assertThat(ig.getOrCreateCache("test").query(new SqlFieldsQuery("SELECT a,b,c FROM TestIndexObject limit 1")).getAll(), is(not(empty())));
        }
    }

    /**
     * @throws Exception if failed.
     */
    public void testMergeChangedConfigOnInactiveGrid() throws Exception {
        {
            //given: two started nodes with test table
            Ignite ig = startGrid(0);
            startGrid(1);

            ig.cluster().active(true);
            LinkedHashMap<String, String> fields = new LinkedHashMap<>();
            fields.put("A", "java.lang.Integer");
            fields.put("B", "java.lang.String");

            CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>("test");

            ccfg.setQueryEntities(Arrays.asList(
                new QueryEntity()
                    .setKeyType("java.lang.Integer")
                    .setValueType("TestIndexObject")
                    .setFields(fields)
            ));

            IgniteCache cache = ig.getOrCreateCache(ccfg);

            fillTestData(ig);

            cache.query(new SqlFieldsQuery("create index myindexb on TestIndexObject(b)")).getAll();

            //and: stop one node and create index on other node
            stopGrid(1);

            cache.query(new SqlFieldsQuery("create index myindexa on TestIndexObject(a)")).getAll();
            cache.query(new SqlFieldsQuery("drop index myindexb")).getAll();
            cache.query(new SqlFieldsQuery("alter table TestIndexObject drop column b")).getAll();

            //and: stop all grid
            stopAllGrids();
        }

        {
            //and: start cluster
            IgniteEx ig = startGrid(0);
            startGrid(1);

            ig.cluster().active(true);

            //then: config should be merged
            try (IgniteDataStreamer<Object, Object> s = ig.dataStreamer("test")) {
                s.allowOverwrite(true);
                for (int i = 0; i < 5_000; i++) {
                    BinaryObject bo = ig.binary().builder("TestIndexObject")
                        .setField("a", i, Object.class)
                        .setField("b", String.valueOf(i), Object.class)
                        .build();

                    s.addData(i, bo);
                }
            }
            IgniteCache<Object, Object> cache = ig.getOrCreateCache("test");

            //then: index "myindexa" and column "b" restored from node "1"
            assertThat(doExplainPlan(cache, "explain select * from TestIndexObject where a > 5"), containsString("myindexa"));
            assertThat(doExplainPlan(cache, "explain select * from TestIndexObject where b > 5"), containsString("myindexb"));
            assertThat(ig.getOrCreateCache("test").query(new SqlFieldsQuery("SELECT a,b FROM TestIndexObject limit 1")).getAll(), is(not(empty())));
        }

    }

    /**
     * @throws Exception if failed.
     */
    public void testTakeChangedConfigOnActiveGrid() throws Exception {
        {
            //given: two started nodes with test table
            Ignite ig = startGrid(0);
            startGrid(1);

            ig.cluster().active(true);

            IgniteCache cache = ig.getOrCreateCache(getTestTableConfiguration());

            fillTestData(ig);

            //stop one node and create index on other node
            stopGrid(1);

            cache.query(new SqlFieldsQuery("create index myindexa on TestIndexObject(a)")).getAll();
            cache.query(new SqlFieldsQuery("create index myindexb on TestIndexObject(b)")).getAll();
            cache.query(new SqlFieldsQuery("alter table TestIndexObject add column (c int)")).getAll();

            //and: stop all grid
            stopGrid(0);
        }

        {
            //and: start cluster
            IgniteEx ig = startGrid(0);
            ig.cluster().active(true);

            ig = startGrid(1);

            //then: config should be merged
            try (IgniteDataStreamer<Object, Object> s = ig.dataStreamer("test")) {
                s.allowOverwrite(true);
                for (int i = 0; i < 5_000; i++) {
                    BinaryObject bo = ig.binary().builder("TestIndexObject")
                        .setField("a", i, Object.class)
                        .setField("b", String.valueOf(i), Object.class)
                        .setField("c", i, Object.class)
                        .build();

                    s.addData(i, bo);
                }
            }
            IgniteCache<Object, Object> cache = ig.getOrCreateCache("test");

            cache.indexReadyFuture().get();

            assertThat(doExplainPlan(cache, "explain select * from TestIndexObject where a > 5"), containsString("myindexa"));
            assertThat(cache.query(new SqlFieldsQuery("SELECT a,b,c FROM TestIndexObject limit 1")).getAll(), is(not(empty())));
        }
    }

    /**
     * @throws Exception if failed.
     */
    public void testFailJoiningNodeBecauseDifferentSql() throws Exception {
        {
            //given: two started nodes with test table
            Ignite ig = startGrid(0);
            startGrid(1);

            ig.cluster().active(true);

            IgniteCache cache = ig.getOrCreateCache(getTestTableConfiguration());

            cache.query(new SqlFieldsQuery("create index myindexa on TestIndexObject(a)")).getAll();

            //stop one node and create index on other node
            stopGrid(1);

            cache.query(new SqlFieldsQuery("drop index myindexa")).getAll();
            cache.query(new SqlFieldsQuery("alter table TestIndexObject drop column b")).getAll();
            cache.query(new SqlFieldsQuery("alter table TestIndexObject add column (b int)")).getAll();
            cache.query(new SqlFieldsQuery("create index myindexa on TestIndexObject(b)")).getAll();

            //and: stopped all grid
            stopAllGrids();
        }

        {
            //and: start cluster
            startGrid(0);
            try {
                startGrid(1);

                fail("Node should start with fail");
            }
            catch (Exception e) {
                String cause = cause(e);
                assertThat(cause, containsString("fieldType of B is different"));
                assertThat(cause, containsString("index MYINDEXA is different"));
            }
        }

    }

    /**
     * @throws Exception if failed.
     */
    public void testFailJoiningNodeBecauseNeedConfigUpdateOnActiveGrid() throws Exception {
        {
            startGrid(0);
            startGrid(1);

            CacheConfiguration<Object, Object> ccfg = getTestTableConfiguration();

            Ignite ig = ignite(0);

            ig.cluster().active(true);

            IgniteCache cache = ig.getOrCreateCache(ccfg);

            fillTestData(ig);

            stopGrid(1);

            cache.query(new SqlFieldsQuery("create index myindexa on TestIndexObject(a)")).getAll();
            cache.query(new SqlFieldsQuery("create index myindexb on TestIndexObject(b)")).getAll();

            stopGrid(0);
        }

        {
            IgniteEx ig = startGrid(1);
            ig.cluster().active(true);

            try {
                startGrid(0);

                fail("Node should start with fail");
            }
            catch (Exception e) {
                assertThat(cause(e), containsString("Node join was fail because"));
            }
        }
    }

    /**
     * get first cause of throwable
     */
    private String cause(Throwable throwable) {
        return throwable.getCause() == null ? throwable.getMessage() : cause(throwable.getCause());
    }

    /**
     * @return result of explain plan
     */
    @NotNull private String doExplainPlan(IgniteCache<Object, Object> cache, String sql) {
        return cache.query(new SqlFieldsQuery(sql)).getAll().get(0).get(0).toString().toLowerCase();
    }

    /**
     * fill data by default
     */
    private void fillTestData(Ignite ig) {
        try (IgniteDataStreamer<Object, Object> s = ig.dataStreamer("test")) {
            for (int i = 0; i < 50_000; i++) {
                BinaryObject bo = ig.binary().builder("TestIndexObject")
                    .setField("a", i, Object.class)
                    .setField("b", String.valueOf(i), Object.class)
                    .build();

                s.addData(i, bo);
            }
        }
    }

    /**
     * @return cache configuration with test table
     */
    @NotNull private CacheConfiguration<Object, Object> getTestTableConfiguration() {
        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("a", "java.lang.Integer");
        fields.put("B", "java.lang.String");

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>("test");

        ccfg.setQueryEntities(Collections.singletonList(
            new QueryEntity()
                .setKeyType("java.lang.Integer")
                .setValueType("TestIndexObject")
                .setFields(fields)
        ));
        return ccfg;
    }
}
