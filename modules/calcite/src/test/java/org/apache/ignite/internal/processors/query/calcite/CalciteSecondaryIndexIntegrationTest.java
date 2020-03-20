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

import java.util.Collections;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
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
        projEntity.setIndexes(Collections.singleton(new QueryIndex("name", true)));
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
    public void testIndexSortedness() {
        QueryEngine engine = Commons.lookupComponent(grid(0).context(), QueryEngine.class);

        System.out.println("No sort: scan should be selected.");
        List<FieldsQueryCursor<List<?>>> cursors = engine.query(null, "PUBLIC", "SELECT * FROM Project");

        for (List<?> row :  cursors.get(0))
            System.out.println(row);

//        System.out.println("Sort is in the same direction as index: index scan should be selected.");
//        grid(0).context().query().getQueryEngine().query(QueryContext.of(), "SELECT * FROM Project ORDER BY name");
//
//        System.out.println("Sort is in the opposite direction as index: table scan with sort should be selected.");
//        grid(0).context().query().getQueryEngine().query(QueryContext.of(), "SELECT * FROM Project ORDER BY name DESC");
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
