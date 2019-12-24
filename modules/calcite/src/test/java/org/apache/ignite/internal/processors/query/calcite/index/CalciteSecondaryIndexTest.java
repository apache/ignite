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
package org.apache.ignite.internal.processors.query.calcite.index;

import java.util.Collections;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.query.QueryContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class CalciteSecondaryIndexTest extends GridCommonAbstractTest {

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


    @Test
    public void testIndexSortedness() {
        System.out.println("No sort: scan should be selected.");
        grid(0).context().query().getQueryEngine().query(QueryContext.of(), "SELECT * FROM Project");

        System.out.println("Sort is in the same direction as index: index scan should be selected.");
        grid(0).context().query().getQueryEngine().query(QueryContext.of(), "SELECT * FROM Project ORDER BY name");

        System.out.println("Sort is in the opposite direction as index: table scan with sort should be selected.");
        grid(0).context().query().getQueryEngine().query(QueryContext.of(), "SELECT * FROM Project ORDER BY name DESC");
    }

    @Test
    public void testIndexFiltering() {
//        grid(0).context().query().getQueryEngine().query(QueryContext.of(),
//            "SELECT * FROM Project WHERE (name = 'Ignite' OR (id > 1 AND NOT id =3)) AND (ver > id AND name > 'A') AND (id BETWEEN 1 AND 3) AND ver IN (1,2) AND  3 = ver AND id = ? ORDER BY name");

        System.out.println("Equals on id, so PK should be selected");
        grid(0).context().query().getQueryEngine().query(QueryContext.of(), "SELECT * FROM Project WHERE  id = 1");

        System.out.println("Equals on name, so NAME_IDX should be selected");
        grid(0).context().query().getQueryEngine().query(QueryContext.of(), "SELECT * FROM Project WHERE  name = 'Ignite'");

        System.out.println("Equals on name, inequality on id, so NAME_IDX should be selected");
        grid(0).context().query().getQueryEngine().query(QueryContext.of(), "SELECT * FROM Project WHERE  name = 'Ignite' AND id > 1");

        System.out.println("Inequality on name, equality on id, so PK should be selected");
        grid(0).context().query().getQueryEngine().query(QueryContext.of(), "SELECT * FROM Project WHERE  name > 'Ignite' AND id = 1");



        System.out.println("Equals on name and id, sort on name, so NAME_IDX should be selected");
        grid(0).context().query().getQueryEngine().query(QueryContext.of(), "SELECT * FROM Project WHERE  name = 'Ignite' AND id = 1 ORDER BY name");

        long start = System.currentTimeMillis();

        System.out.println("Equals on name and id, sort on id, so PK should be selected");
        grid(0).context().query().getQueryEngine().query(QueryContext.of(), "SELECT * FROM Project WHERE  name = 'Ignite' AND id = 1 ORDER BY id");

        System.out.println("planning time=" + (System.currentTimeMillis() - start));
    }

    private CacheConfiguration<Object, Object> cache(QueryEntity ent) {
        return new CacheConfiguration<>(ent.getTableName())
            .setCacheMode(CacheMode.REPLICATED)
            .setBackups(0)
            .setQueryEntities(Collections.singletonList(ent))
            .setSqlSchema("PUBLIC");
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
