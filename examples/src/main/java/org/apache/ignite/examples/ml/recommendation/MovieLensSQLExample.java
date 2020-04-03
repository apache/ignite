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

package org.apache.ignite.examples.ml.recommendation;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.examples.ml.util.MLSandboxDatasets;
import org.apache.ignite.examples.ml.util.SandboxMLCache;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.inference.IgniteModelStorageUtil;
import org.apache.ignite.ml.recommendation.RecommendationModel;
import org.apache.ignite.ml.recommendation.RecommendationTrainer;
import org.apache.ignite.ml.sql.SQLFunctions;
import org.apache.ignite.ml.sql.SqlDatasetBuilder;

/**
 * Example of recommendation system based on MovieLens dataset (see https://grouplens.org/datasets/movielens/) and SQL.
 * In this example we create a cache with MovieLens rating data. Each entry in this cache represents a rating point
 * (rating set by a single user to a single movie). Then we pass this cache to {@link RecommendationTrainer} and so
 * that train {@link RecommendationModel}. This model predicts rating with assumed to be set by any user to any movie.
 * When model is ready we calculate R2 score.
 */
public class MovieLensSQLExample {
    /** Dummy cache name. */
    private static final String DUMMY_CACHE_NAME = "dummy_cache";

    /** Run example. */
    public static void main(String[] args) throws IOException {
        System.out.println();
        System.out.println(">>> Recommendation system over cache based dataset usage example started.");
        // Start ignite grid.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite-ml.xml")) {
            System.out.println(">>> Ignite grid started.");

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
                    "    movie_id int,\n" +
                    "    user_id int,\n" +
                    "    rating float\n" +
                    ") with \"template=partitioned\";")).getAll();

                System.out.println(">>> Loading data...");
                loadMovieLensDataset(ignite, cache, 10_000);

                LearningEnvironmentBuilder envBuilder = LearningEnvironmentBuilder.defaultBuilder().withRNGSeed(1);
                RecommendationTrainer trainer = new RecommendationTrainer()
                    .withMaxIterations(100)
                    .withBatchSize(10)
                    .withLearningRate(10)
                    .withLearningEnvironmentBuilder(envBuilder)
                    .withTrainerEnvironment(envBuilder.buildForTrainer());

                System.out.println(">>> Training model...");
                RecommendationModel<Serializable, Serializable> mdl = trainer.fit(
                    new SqlDatasetBuilder(ignite, "SQL_PUBLIC_RATINGS"),
                    "movie_id",
                    "user_id",
                    "rating"
                );

                System.out.println("Saving model into model storage...");
                IgniteModelStorageUtil.saveModel(ignite, mdl, "movielens_model");

                System.out.println("Inference...");
                try (QueryCursor<List<?>> cursor = cache.query(new SqlFieldsQuery("select " +
                    "rating, " +
                    "predictRecommendation('movielens_model', movie_id, user_id) as prediction " +
                    "from ratings"))) {
                    for (List<?> row : cursor) {
                        double rating = (Double)row.get(0);
                        double prediction = (Double)row.get(1);
                        System.out.println("Rating: " + rating + ", prediction: " + prediction);
                    }
                }
            }
            finally {
                cache.query(new SqlFieldsQuery("DROP TABLE ratings"));
                cache.destroy();
            }
        }
        finally {
            System.out.flush();
        }
    }

    /**
     * Loads MovieLens dataset into cache.
     *
     * @param ignite Ignite instance.
     * @param cnt Number of rating point to be loaded.
     * @throws IOException If dataset not found.
     */
    private static void loadMovieLensDataset(Ignite ignite, IgniteCache<?, ?> cache, int cnt) throws IOException {
        SqlFieldsQuery qry = new SqlFieldsQuery("insert into ratings (rating_id, movie_id, user_id, rating) values (?, ?, ?, ?)");
        int seq = 0;
        for (String s : new SandboxMLCache(ignite).loadDataset(MLSandboxDatasets.MOVIELENS)) {
            String[] line = s.split(",");

            int userId = Integer.valueOf(line[0]);
            int movieId = Integer.valueOf(line[1]);
            double rating = Double.valueOf(line[2]);

            qry.setArgs(seq++, movieId, userId, rating);
            cache.query(qry);

            if (seq == cnt)
                break;
        }
    }
}
