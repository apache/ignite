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

package org.apache.ignite.examples.ml.sql;

import java.io.IOException;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.ml.dataset.feature.extractor.impl.BinaryObjectVectorizer;
import org.apache.ignite.ml.inference.IgniteModelStorageUtil;
import org.apache.ignite.ml.sql.SQLFunctions;
import org.apache.ignite.ml.sql.SqlDatasetBuilder;
import org.apache.ignite.ml.tree.DecisionTreeClassificationTrainer;
import org.apache.ignite.ml.tree.DecisionTreeNode;

import static org.apache.ignite.examples.ml.sql.DecisionTreeClassificationTrainerSQLTableExample.loadTitanicDatasets;

/**
 * Example of using distributed {@link DecisionTreeClassificationTrainer} on a data stored in SQL table and inference
 * made as SQL select query.
 */
public class DecisionTreeClassificationTrainerSQLInferenceExample {
    /**
     * Dummy cache name.
     */
    private static final String DUMMY_CACHE_NAME = "dummy_cache";

    /**
     * Run example.
     */
    public static void main(String[] args) throws IOException {
        System.out.println(">>> Decision tree classification trainer example started.");

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
                cache.query(new SqlFieldsQuery("create table titanic_train (\n" +
                    "    passengerid int primary key,\n" +
                    "    pclass int,\n" +
                    "    survived int,\n" +
                    "    name varchar(255),\n" +
                    "    sex varchar(255),\n" +
                    "    age float,\n" +
                    "    sibsp int,\n" +
                    "    parch int,\n" +
                    "    ticket varchar(255),\n" +
                    "    fare float,\n" +
                    "    cabin varchar(255),\n" +
                    "    embarked varchar(255)\n" +
                    ") with \"template=partitioned\";")).getAll();

                System.out.println(">>> Creating table with test data...");
                cache.query(new SqlFieldsQuery("create table titanic_test (\n" +
                    "    passengerid int primary key,\n" +
                    "    pclass int,\n" +
                    "    survived int,\n" +
                    "    name varchar(255),\n" +
                    "    sex varchar(255),\n" +
                    "    age float,\n" +
                    "    sibsp int,\n" +
                    "    parch int,\n" +
                    "    ticket varchar(255),\n" +
                    "    fare float,\n" +
                    "    cabin varchar(255),\n" +
                    "    embarked varchar(255)\n" +
                    ") with \"template=partitioned\";")).getAll();

                loadTitanicDatasets(ignite, cache);

                System.out.println(">>> Prepare trainer...");
                DecisionTreeClassificationTrainer trainer = new DecisionTreeClassificationTrainer(4, 0);

                System.out.println(">>> Perform training...");
                DecisionTreeNode mdl = trainer.fit(
                    new SqlDatasetBuilder(ignite, "SQL_PUBLIC_TITANIC_TRAIN"),
                    new BinaryObjectVectorizer<>("pclass", "age", "sibsp", "parch", "fare")
                        .withFeature("sex", BinaryObjectVectorizer.Mapping.create().map("male", 1.0).defaultValue(0.0))
                        .labeled("survived")
                );

                System.out.println(">>> Saving model...");

                // Model storage is used to store raw serialized model.
                System.out.println("Saving model into model storage...");
                IgniteModelStorageUtil.saveModel(ignite, mdl, "titanic_model_tree");

                // Making inference using saved model.
                System.out.println("Inference...");
                try (QueryCursor<List<?>> cursor = cache.query(new SqlFieldsQuery("select " +
                    "survived as truth, " +
                    "predict('titanic_model_tree', pclass, age, sibsp, parch, fare, case sex when 'male' then 1 else 0 end) as prediction " +
                    "from titanic_train"))) {
                    // Print inference result.
                    System.out.println("| Truth | Prediction |");
                    System.out.println("|--------------------|");
                    for (List<?> row : cursor)
                        System.out.println("|     " + row.get(0) + " |        " + row.get(1) + " |");
                }

                IgniteModelStorageUtil.removeModel(ignite, "titanic_model_tree");
            }
            finally {
                cache.query(new SqlFieldsQuery("DROP TABLE titanic_train"));
                cache.query(new SqlFieldsQuery("DROP TABLE titanic_test"));
                cache.destroy();
            }
        }
        finally {
            System.out.flush();
        }
    }
}
