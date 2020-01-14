package org.apache.ignite.internal.processors.query.calcite;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class CalciteQueryProcessorTest extends GridCommonAbstractTest {
    /** */
    private IgniteEx ignite;

    @Before
    public void setup() throws Exception {
        ignite = startGrids(5);
    }

    @After
    public void tearDown() throws Exception {
        stopAllGrids();
    }

    @Test
    public void query() throws Exception {
        IgniteCache<Integer, Developer> developer = ignite.getOrCreateCache(new CacheConfiguration<Integer, Developer>()
            .setName("developer")
            .setSqlSchema("PUBLIC")
            .setIndexedTypes(Integer.class, Developer.class)
            .setBackups(2)
        );

        IgniteCache<Integer, Project> project = ignite.getOrCreateCache(new CacheConfiguration<Integer, Project>()
            .setName("project")
            .setSqlSchema("PUBLIC")
            .setIndexedTypes(Integer.class, Project.class)
            .setBackups(2)
        );

        project.putAll(ImmutableMap.of(
            0, new Project("Ignite"),
            1, new Project("Calcite")
        ));

        developer.putAll(ImmutableMap.of(
            0, new Developer("Igor", 1),
            1, new Developer("Roman", 0)
        ));

        doSleep(1000);

        QueryEngine engine = Commons.lookupComponent(ignite.context(), QueryEngine.class);

        List<FieldsQueryCursor<List<?>>> query = engine.query(null, "PUBLIC", "select * from DEVELOPER d, PROJECT p where d.projectId = p._key and d._key = ?", 1);

        F.first(query).getAll();
    }

    public static class Developer {
        @QuerySqlField
        public String name;
        @QuerySqlField
        public Integer projectId;

        public Developer(String name, Integer projectId) {
            this.name = name;
            this.projectId = projectId;
        }

        public Developer(String name) {
            this.name = name;
        }
    }

    public static class Project {
        @QuerySqlField
        public String name;

        public Project(String name) {
            this.name = name;
        }
    }
}