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
package org.apache.ignite.internal.processors.query.calcite;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * TODO: Add class description.
 */
public class CalciteSecondaryIndexIntegrationTest extends GridCommonAbstractTest {

    @Override protected void beforeTestsStarted() throws Exception {
        Ignite grid = startGridsMultiThreaded(1);

        QueryEntity projEntity = new QueryEntity();
        projEntity.setKeyType(Integer.class.getName());
        projEntity.setKeyFieldName("id");
        projEntity.setValueType(Project.class.getName());
        projEntity.addQueryField("ver", Integer.class.getName(), null);
        projEntity.addQueryField("name", String.class.getName(), null);
        projEntity.addQueryField("id", Integer.class.getName(), null);
        QueryIndex simpleIdx = new QueryIndex("name", true);
        LinkedHashMap<String, Boolean> fields = new LinkedHashMap<>();
        fields.put("ver", false);
        fields.put("name", true);
        fields.put("id", false);
        QueryIndex complexIdx = new QueryIndex(fields, QueryIndexType.SORTED);
        projEntity.setIndexes(Arrays.asList(simpleIdx, complexIdx));
        projEntity.setTableName("Project");

        CacheConfiguration projCfg = cache(projEntity);


        IgniteCache projCache = grid.createCache(projCfg);

        projCache.put(1, new Project("Optiq", 3));
        projCache.put(2, new Project("Ignite", 3));
        projCache.put(3, new Project("Calcite", 1));
        projCache.put(4, new Project("GridGain", 2));

        awaitPartitionMapExchange();
    }

    private CacheConfiguration<Object, Object> cache(QueryEntity ent) {
        return new CacheConfiguration<>(ent.getTableName())
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(0)
            .setQueryEntities(Collections.singletonList(ent))
            .setSqlSchema("PUBLIC");
    }

    @Test
    public void testPkIdx_KeyColumnFilter() {
        assertIndexInQuery("PROJECT", "SELECT * FROM Project WHERE _key=1");
    }

    @Test
    public void testPkIdx_KeyColumnAliasFilter() {
        assertIndexInQuery("PROJECT", "SELECT * FROM Project WHERE id=1");
    }

    @Test
    public void testNameIdx_NameFilter() {
        assertIndexInQuery("PROJECT_NAME_ASC_IDX", "SELECT * FROM Project WHERE name='Ignite'");
    }

    @Test
    public void testVerNameIdIdx_VerFilter() {
        assertIndexInQuery("PROJECT_VER_DESC_NAME_ASC_ID_DESC_IDX", "SELECT * FROM Project WHERE ver=1 AND name='Vasya'");
    }

//    @Test
//    public void testIndexSortedness() {
//        QueryEngine engine = Commons.lookupComponent(grid(0).context(), QueryEngine.class);
//
//        System.out.println("No sort: scan should be selected.");
//        List<FieldsQueryCursor<List<?>>> cursors = engine.query(null, "PUBLIC", "SELECT * FROM Project");
//
//        for (List<?> row :  cursors.get(0))
//            System.out.println(row);
//
////        System.out.println("Sort is in the same direction as index: index scan should be selected.");
////        grid(0).context().query().getQueryEngine().query(QueryContext.of(), "SELECT * FROM Project ORDER BY name");
////
////        System.out.println("Sort is in the opposite direction as index: table scan with sort should be selected.");
////        grid(0).context().query().getQueryEngine().query(QueryContext.of(), "SELECT * FROM Project ORDER BY name DESC");
//    }




//        System.out.println("Equals on name, inequality on id, so NAME_IDX should be selected");
//        grid(0).context().query().getQueryEngine().query(QueryContext.of(), "SELECT * FROM Project WHERE  name = 'Ignite' AND id > 1");
//
//        System.out.println("Inequality on name, equality on id, so PK should be selected");
//        grid(0).context().query().getQueryEngine().query(QueryContext.of(), "SELECT * FROM Project WHERE  name > 'Ignite' AND id = 1");
//
//
//
//        System.out.println("Equals on name and id, sort on name, so NAME_IDX should be selected");
//        grid(0).context().query().getQueryEngine().query(QueryContext.of(), "SELECT * FROM Project WHERE  name = 'Ignite' AND id = 1 ORDER BY name");
//
//        long start = System.currentTimeMillis();
//
//        System.out.println("Equals on name and id, sort on id, so PK should be selected");
//        grid(0).context().query().getQueryEngine().query(QueryContext.of(), "SELECT * FROM Project WHERE  name = 'Ignite' AND id = 1 ORDER BY id");
//
//        System.out.println("planning time=" + (System.currentTimeMillis() - start));

    public void assertIndexInQuery(String idxName, String qry) {
        QueryEngine engine = Commons.lookupComponent(grid(0).context(), QueryEngine.class);
//
//        System.out.println("Equals on id, so PK should be selected");
//        engine.query(null, "PUBLIC", "SELECT * FROM Project WHERE  id = 1");

        System.out.println("Equals on name, so NAME_IDX should be selected");
        List<FieldsQueryCursor<List<?>>> cursors =
            engine.query(null, "PUBLIC", "EXPLAIN PLAN FOR " + qry);

        FieldsQueryCursor<List<?>> cur = cursors.get(0);

        List<List<?>> res = cur.getAll();

        String plan = (String)res.get(0).get(0);

        String idxScanName = "IgniteTableScan(table=[[PUBLIC, " + idxName + "]],";

        assertTrue("idxName=" + idxName + ", plan=" + plan, plan.contains(idxScanName));
    }


    private static class Project {
        String name;
        int ver;

        public Project(String name, int ver) {
            this.name = name;
            this.ver = ver;
        }

        @Override public String toString() {
            return "Project{" +
                "name='" + name + '\'' +
                ", ver=" + ver +
                '}';
        }
    }
}
