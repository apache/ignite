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

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.inference.IgniteModelStorageUtil;
import org.apache.ignite.ml.inference.ModelDescriptor;
import org.apache.ignite.ml.inference.ModelSignature;
import org.apache.ignite.ml.inference.parser.IgniteModelParser;
import org.apache.ignite.ml.inference.reader.ModelStorageModelReader;
import org.apache.ignite.ml.inference.storage.descriptor.ModelDescriptorStorage;
import org.apache.ignite.ml.inference.storage.descriptor.ModelDescriptorStorageFactory;
import org.apache.ignite.ml.inference.storage.model.ModelStorage;
import org.apache.ignite.ml.inference.storage.model.ModelStorageFactory;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.sql.SQLFeatureExtractor;
import org.apache.ignite.ml.sql.SQLFunctions;
import org.apache.ignite.ml.sql.SQLLabelExtractor;
import org.apache.ignite.ml.sql.SqlDatasetBuilder;
import org.apache.ignite.ml.tree.DecisionTreeClassificationTrainer;
import org.apache.ignite.ml.tree.DecisionTreeNode;
import org.apache.spark.util.Utils;

/**
 * Example of using distributed {@link DecisionTreeClassificationTrainer} on a data stored in SQL table and inference
 * made as SQL select query.
 */
public class DecisionTreeClassificationTrainerSQLInferenceExample {
    /** Dummy cache name. */
    private static final String DUMMY_CACHE_NAME = "dummy_cache";

    /** Training data. */
    private static final String TRAIN_DATA_RES = "examples/src/main/resources/datasets/titanik_train.csv";

    /** Test data. */
    private static final String TEST_DATA_RES = "examples/src/main/resources/datasets/titanik_test.csv";

    /** Run example. */
    public static void main(String[] args) {
        System.out.println(">>> Decision tree classification trainer example started.");

        // Start ignite grid.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite-ml.xml")) {
            System.out.println(">>> Ignite grid started.");

            // Dummy cache is required to perform SQL queries.
            CacheConfiguration<?, ?> cacheCfg = new CacheConfiguration<>(DUMMY_CACHE_NAME)
                .setSqlSchema("PUBLIC")
                .setSqlFunctionClasses(SQLFunctions.class);

            IgniteCache<?, ?> cache = ignite.createCache(cacheCfg);

            System.out.println(">>> Creating table with training data...");
            cache.query(new SqlFieldsQuery("create table titanik_train (\n" +
                "    passengerid int primary key,\n" +
                "    survived int,\n" +
                "    pclass int,\n" +
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

            System.out.println(">>> Filling training data...");
            cache.query(new SqlFieldsQuery("insert into titanik_train select * from csvread('" +
                IgniteUtils.resolveIgnitePath(TRAIN_DATA_RES).getAbsolutePath() + "')")).getAll();

            System.out.println(">>> Creating table with test data...");
            cache.query(new SqlFieldsQuery("create table titanik_test (\n" +
                "    passengerid int primary key,\n" +
                "    pclass int,\n" +
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

            System.out.println(">>> Filling training data...");
            cache.query(new SqlFieldsQuery("insert into titanik_test select * from csvread('" +
                IgniteUtils.resolveIgnitePath(TEST_DATA_RES).getAbsolutePath() + "')")).getAll();

            System.out.println(">>> Prepare trainer...");
            DecisionTreeClassificationTrainer trainer = new DecisionTreeClassificationTrainer(4, 0);

            System.out.println(">>> Perform training...");
            DecisionTreeNode mdl = trainer.fit(
                new SqlDatasetBuilder(ignite, "SQL_PUBLIC_TITANIK_TRAIN"),
                new SQLFeatureExtractor()
                    .withField("pclass")
                    .withField("sex", e -> "male".equals(e) ? 1 : 0)
                    .withField("age")
                    .withField("sibsp")
                    .withField("parch")
                    .withField("fare"),
                new SQLLabelExtractor("survived")
            );

            System.out.println(">>> Saving model...");

            // Model storage is used to store raw serialized model.
            System.out.println("Saving model into model storage...");
            IgniteModelStorageUtil.save(mdl, "titanik_model_tree");

            // Making inference using saved model.
            System.out.println("Inference...");
            try (QueryCursor<List<?>> cursor = cache.query(new SqlFieldsQuery("select " +
                "survived as truth, " +
                "predict('my_model', pclass, case sex when 'male' then 1 else 0 end, age, sibsp, parch, fare) as prediction " +
                "from titanik_train"))) {
                // Print inference result.
                System.out.println("| Truth | Prediction |");
                System.out.println("|--------------------|");
                for (List<?> row : cursor)
                    System.out.println("|     " + row.get(0) + " |        " + row.get(1) + " |");
            }
        }
    }

    /**
     * Replaces NULL values by 0.
     *
     * @param obj Input value.
     * @param <T> Type of value.
     * @return Input value of 0 if value is null.
     */
    private static <T extends Number> double replaceNull(T obj) {
        if (obj == null)
            return 0;

        return obj.doubleValue();
    }
}
