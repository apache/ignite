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

package org.apache.ignite.ml.mleap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import ml.combust.mleap.core.types.ScalarType;
import ml.combust.mleap.core.types.StructField;
import ml.combust.mleap.core.types.StructType;
import ml.combust.mleap.runtime.frame.DefaultLeapFrame;
import ml.combust.mleap.runtime.frame.Row;
import ml.combust.mleap.runtime.frame.Transformer;
import ml.combust.mleap.runtime.javadsl.LeapFrameBuilder;
import org.apache.ignite.ml.inference.Model;
import scala.collection.immutable.Set;
import scala.collection.immutable.Stream;
import scala.util.Try;

/**
 * MLeap model imported and wrapped to be compatible with Apache Ignite infrastructure.
 */
public class MLeapModel implements Model<HashMap<String, Double>, Double> {
    /** MLpeap model (transformer in terms of MLeap). */
    private final Transformer transformer;

    /** List of field names. */
    private final List<String> schema;

    /**
     * Constructs a new instance of MLeap model.
     *
     * @param transformer MLpeap model (transformer in terms of MLeap).
     * @param schema List of field names.
     */
    public MLeapModel(Transformer transformer, List<String> schema) {
        this.transformer = transformer;
        this.schema = schema;
    }

    /** {@inheritDoc} */
    @Override public Double predict(HashMap<String, Double> input) {
        LeapFrameBuilder builder = new LeapFrameBuilder();
        List<StructField> fs = new ArrayList<>();

        for (String fieldName : input.keySet())
            fs.add(new StructField(fieldName, ScalarType.Double()));

        StructType schema = builder.createSchema(fs);

        List<Object> values = new ArrayList<>(input.values());
        List<Row> rows = new ArrayList<>();
        rows.add(builder.createRowFromIterable(values));


        DefaultLeapFrame inputFrame = builder.createFrame(schema, rows);

        return predict(inputFrame);
    }

    public double predict(Double[] input) {
        LeapFrameBuilder builder = new LeapFrameBuilder();

        List<StructField> fs = new ArrayList<>();

        for (String fieldName : schema)
            fs.add(new StructField(fieldName, ScalarType.Double()));

        StructType schema = builder.createSchema(fs);

        List<Row> rows = new ArrayList<>();
        rows.add(builder.createRowFromIterable(Arrays.asList(input)));

        DefaultLeapFrame inputFrame = builder.createFrame(schema, rows);

        return predict(inputFrame);
    }

    public double predict(DefaultLeapFrame inputFrame) {
        DefaultLeapFrame outputFrame = transformer.transform(inputFrame).get();

        Try<DefaultLeapFrame> res = outputFrame.select(new Set.Set1<>("price_prediction").toSeq());
        DefaultLeapFrame f = res.get();

        Stream<?> stream = (Stream<?>)f.productElement(1);
        Row rrow = (Row)stream.head();

        return (Double)rrow.get(0);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        transformer.close();
    }
}
