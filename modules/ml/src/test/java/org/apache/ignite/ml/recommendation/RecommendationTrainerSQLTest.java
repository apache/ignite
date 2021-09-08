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

package org.apache.ignite.ml.recommendation;

import java.io.Serializable;
import java.util.Random;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.sql.SQLFunctions;
import org.apache.ignite.ml.sql.SqlDatasetBuilder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** Tests for {@link RecommendationTrainer} with binary objects and SQL. */
public class RecommendationTrainerSQLTest extends GridCommonAbstractTest {
    /** Dummy cache name. */
    private static final String DUMMY_CACHE_NAME = "dummy_cache";

    /** Number of nodes in grid. */
    private static final int NODE_COUNT = 3;

    /** Ignite instance. */
    private Ignite ignite;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        for (int i = 1; i <= NODE_COUNT; i++)
            startGrid(i);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() {
        /* Grid instance. */
        ignite = grid(NODE_COUNT);
        ignite.configuration().setPeerClassLoadingEnabled(true);
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());
    }

    /** */
    @Test
    public void testFit() {
        // Dummy cache is required to perform SQL queries.
        CacheConfiguration<?, ?> cacheCfg = new CacheConfiguration<>(DUMMY_CACHE_NAME)
            .setSqlSchema("PUBLIC")
            .setSqlFunctionClasses(SQLFunctions.class);

        IgniteCache<?, ?> cache = null;
        try {
            cache = ignite.getOrCreateCache(cacheCfg);

            System.out.println(">>> Creating table with training data...");
            cache.query(new SqlFieldsQuery("create table ratings (\n" +
                "    rating_id int primary key,\n" +
                "    obj_id int,\n" +
                "    subj_id int,\n" +
                "    rating float\n" +
                ") with \"template=partitioned\";")).getAll();

            int size = 100;
            Random rnd = new Random(0L);
            SqlFieldsQuery qry = new SqlFieldsQuery("insert into ratings (rating_id, obj_id, subj_id, rating) values (?, ?, ?, ?)");
            // Quadrant I contains "0", quadrant II contains "1", quadrant III contains "0", quadrant IV contains "1".
            for (int i = 0; i < size; i++) {
                for (int j = 0; j < size; j++) {
                    if (rnd.nextBoolean()) {
                        double rating = ((i > size / 2) ^ (j > size / 2)) ? 1.0 : 0.0;
                        qry.setArgs(i * size + j, i, j, rating);
                        cache.query(qry);
                    }
                }
            }

            RecommendationTrainer trainer = new RecommendationTrainer()
                .withMaxIterations(100)
                .withLearningRate(50.0)
                .withBatchSize(10)
                .withK(2)
                .withLearningEnvironmentBuilder(LearningEnvironmentBuilder.defaultBuilder().withRNGSeed(1))
                .withTrainerEnvironment(LearningEnvironmentBuilder.defaultBuilder().withRNGSeed(1).buildForTrainer());

            RecommendationModel<Serializable, Serializable> mdl = trainer.fit(
                new SqlDatasetBuilder(ignite, "SQL_PUBLIC_RATINGS"),
                "obj_id",
                "subj_id",
                "rating"
            );

            int incorrect = 0;
            for (int i = 0; i < size; i++) {
                for (int j = 0; j < size; j++) {
                    if (rnd.nextBoolean()) {
                        double rating = ((i > size / 2) ^ (j > size / 2)) ? 1.0 : 0.0;
                        double prediction = mdl.predict(new ObjectSubjectPair<>(i, j));
                        if (Math.abs(prediction - rating) >= 1e-5)
                            incorrect++;
                    }
                }
            }

            assertEquals(0, incorrect);
        }
        finally {
            cache.query(new SqlFieldsQuery("DROP TABLE ratings"));
            cache.destroy();
        }
    }
}
