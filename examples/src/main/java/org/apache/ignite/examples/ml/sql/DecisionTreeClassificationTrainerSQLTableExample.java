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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.examples.ml.util.MLSandboxDatasets;
import org.apache.ignite.examples.ml.util.SandboxMLCache;
import org.apache.ignite.ml.dataset.feature.extractor.impl.BinaryObjectVectorizer;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.sql.SqlDatasetBuilder;
import org.apache.ignite.ml.tree.DecisionTreeClassificationTrainer;
import org.apache.ignite.ml.tree.DecisionTreeNode;

/**
 * Example of using distributed {@link DecisionTreeClassificationTrainer} on a data stored in SQL table.
 */
public class DecisionTreeClassificationTrainerSQLTableExample {
    /**
     * Dummy cache name.
     */
    private static final String DUMMY_CACHE_NAME = "dummy_cache";

    /**
     * Run example.
     */
    public static void main(String[] args) throws IgniteCheckedException, IOException {
        System.out.println(">>> Decision tree classification trainer example started.");

        // Start ignite grid.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");

            // Dummy cache is required to perform SQL queries.
            CacheConfiguration<?, ?> cacheCfg = new CacheConfiguration<>(DUMMY_CACHE_NAME)
                .setSqlSchema("PUBLIC");

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

                System.out.println("Tree is here: " + mdl.toString(true));

                System.out.println(">>> Perform inference...");
                try (QueryCursor<List<?>> cursor = cache.query(new SqlFieldsQuery("select " +
                    "pclass, " +
                    "sex, " +
                    "age, " +
                    "sibsp, " +
                    "parch, " +
                    "fare from titanic_test"))) {
                    for (List<?> passenger : cursor) {
                        Vector input = VectorUtils.of(new Double[]{
                            asDouble(passenger.get(0)),
                            "male".equals(passenger.get(1)) ? 1.0 : 0.0,
                            asDouble(passenger.get(2)),
                            asDouble(passenger.get(3)),
                            asDouble(passenger.get(4)),
                            asDouble(passenger.get(5)),
                        });

                        double prediction = mdl.predict(input);

                        System.out.printf("Passenger %s will %s.\n", passenger, prediction == 0 ? "die" : "survive");
                    }
                }

                System.out.println(">>> Example completed.");
            } finally {
                cache.query(new SqlFieldsQuery("DROP TABLE titanic_train"));
                cache.query(new SqlFieldsQuery("DROP TABLE titanic_test"));
                cache.destroy();
            }
        } finally {
            System.out.flush();
        }
    }

    /**
     * Converts specified number into double.
     *
     * @param obj Number.
     * @param <T> Type of number.
     * @return Double.
     */
    private static <T extends Number> Double asDouble(Object obj) {
        if (obj == null)
            return null;

        if (obj instanceof Number) {
            Number num = (Number) obj;

            return num.doubleValue();
        }

        throw new IllegalArgumentException("Object is expected to be a number [obj=" + obj + "]");
    }

    /**
     * Loads Titanic dataset into cache.
     *
     * @param ignite Ignite instance.
     * @throws IOException If dataset not found.
     */
    static void loadTitanicDatasets(Ignite ignite, IgniteCache<?, ?> cache) throws IOException {

        List<String> titanicDatasetRows = new SandboxMLCache(ignite).loadDataset(MLSandboxDatasets.TITANIC);
        List<String> train = titanicDatasetRows.subList(0, 1000);
        List<String> test = titanicDatasetRows.subList(1000, titanicDatasetRows.size());

        insertToCache(cache, train, "titanic_train");
        insertToCache(cache, test, "titanic_test");
    }

    /** */
    private static void insertToCache(IgniteCache<?, ?> cache, List<String> train, String tableName) {
        SqlFieldsQuery insertTrain = new SqlFieldsQuery("insert into " + tableName + " " +
            "(passengerid, pclass, survived, name, sex, age, sibsp, parch, ticket, fare, cabin, embarked) " +
            "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

        int seq = 0;
        for (String s : train) {
            String[] line = s.split(";");
            int pclass = parseInteger(line[0]);
            int survived = parseInteger(line[1]);
            String name = line[2];
            String sex = line[3];
            double age = parseDouble(line[4]);
            double sibsp = parseInteger(line[5]);
            double parch = parseInteger(line[6]);
            String ticket = line[7];
            double fare = parseDouble(line[8]);
            String cabin = line[9];
            String embarked = line[10];
            insertTrain.setArgs(seq++, pclass, survived, name, sex, age, sibsp, parch, ticket, fare, cabin, embarked);
            cache.query(insertTrain);
        }
    }

    /** */
    private static Integer parseInteger(String value) {
        try {
            return Integer.valueOf(value);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    /** */
    private static Double parseDouble(String value) {
        try {
            return Double.valueOf(value);
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }
}
