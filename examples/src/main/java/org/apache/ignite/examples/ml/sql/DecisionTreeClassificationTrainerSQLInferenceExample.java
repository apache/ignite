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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.dataset.impl.cache.CacheBasedDatasetBuilder;
import org.apache.ignite.ml.inference.Model;
import org.apache.ignite.ml.inference.ModelDescriptor;
import org.apache.ignite.ml.inference.ModelSignature;
import org.apache.ignite.ml.inference.builder.SingleModelBuilder;
import org.apache.ignite.ml.inference.parser.IgniteModelParser;
import org.apache.ignite.ml.inference.reader.ModelStorageModelReader;
import org.apache.ignite.ml.inference.storage.descriptor.ModelDescriptorStorage;
import org.apache.ignite.ml.inference.storage.descriptor.ModelDescriptorStorageFactory;
import org.apache.ignite.ml.inference.storage.model.ModelStorage;
import org.apache.ignite.ml.inference.storage.model.ModelStorageFactory;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.tree.DecisionTreeClassificationTrainer;
import org.apache.ignite.ml.tree.DecisionTreeNode;

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
    public static void main(String[] args) throws IOException {
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
            IgniteCache<Integer, BinaryObject> titanicTrainCache = ignite.cache("SQL_PUBLIC_TITANIK_TRAIN");
            DecisionTreeNode mdl = trainer.fit(
                // We have to specify ".withKeepBinary(true)" because SQL caches contains only binary objects and this
                // information has to be passed into the trainer.
                new CacheBasedDatasetBuilder<>(ignite, titanicTrainCache).withKeepBinary(true),
                (k, v) -> VectorUtils.of(
                    // We have to handle null values here to avoid NpE during unboxing.
                    replaceNull(v.<Integer>field("pclass")),
                    "male".equals(v.<String>field("sex")) ? 1 : 0,
                    replaceNull(v.<Double>field("age")),
                    replaceNull(v.<Integer>field("sibsp")),
                    replaceNull(v.<Integer>field("parch")),
                    replaceNull(v.<Double>field("fare"))
                ),
                (k, v) -> replaceNull(v.<Integer>field("survived"))
            );

            System.out.println(">>> Saving model...");

            // Model storage is used to store raw serialized model.
            System.out.println("Saving model into model storage...");
            byte[] serializedMdl = serialize((IgniteModel<byte[], byte[]>)i -> {
                // Here we need to wrap model so that it accepts and returns byte array.
                try {
                    Vector input = deserialize(i);
                    return serialize(mdl.predict(input));
                }
                catch (IOException | ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
            });

            ModelStorage storage = new ModelStorageFactory().getModelStorage(ignite);
            storage.mkdirs("/");
            storage.putFile("/my_model", serializedMdl);

            // Model descriptor storage that is used to store model metadata.
            System.out.println("Saving model descriptor into model descriptor storage...");
            ModelDescriptor desc = new ModelDescriptor(
                "MyModel",
                "My Cool Model",
                new ModelSignature("", "", ""),
                new ModelStorageModelReader("/my_model"),
                new IgniteModelParser<>()
            );
            ModelDescriptorStorage descStorage = new ModelDescriptorStorageFactory().getModelDescriptorStorage(ignite);
            descStorage.put("my_model", desc);

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

    /**
     * SQL functions that should be defined and passed into cache configuration to extend list of functions available
     * in SQL interface.
     */
    public static class SQLFunctions {
        /**
         * Makes prediction using specified model name to extract model from model storage and specified input values
         * as input object for prediction.
         *
         * @param mdl Pretrained model.
         * @param x Input values.
         * @return Prediction.
         */
        @QuerySqlFunction
        public static double predict(String mdl, Double... x) {
            // Pretrained models work with vector of doubles so we need to replace null by 0 (or any other double).
            for (int i = 0; i < x.length; i++)
                if (x[i] == null)
                    x[i] = 0.0;

            Ignite ignite = Ignition.ignite();

            ModelDescriptorStorage descStorage = new ModelDescriptorStorageFactory().getModelDescriptorStorage(ignite);
            ModelDescriptor desc = descStorage.get(mdl);

            Model<byte[], byte[]> infMdl = new SingleModelBuilder().build(desc.getReader(), desc.getParser());

            Vector input = VectorUtils.of(x);

            try {
                return deserialize(infMdl.predict(serialize(input)));
            }
            catch (IOException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Serialized the specified object.
     *
     * @param o Object to be serialized.
     * @return Serialized object as byte array.
     * @throws IOException In case of exception.
     */
    private static <T extends Serializable> byte[] serialize(T o) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(o);
            oos.flush();

            return baos.toByteArray();
        }
    }

    /**
     * Deserialized object represented as a byte array.
     *
     * @param o Serialized object.
     * @param <T> Type of serialized object.
     * @return Deserialized object.
     * @throws IOException In case of exception.
     * @throws ClassNotFoundException In case of exception.
     */
    @SuppressWarnings("unchecked")
    private static <T extends Serializable> T deserialize(byte[] o) throws IOException, ClassNotFoundException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(o);
             ObjectInputStream ois = new ObjectInputStream(bais)) {

            return (T)ois.readObject();
        }
    }
}
