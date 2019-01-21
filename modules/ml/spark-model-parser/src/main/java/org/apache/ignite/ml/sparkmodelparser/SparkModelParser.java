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

package org.apache.ignite.ml.sparkmodelparser;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.ignite.ml.inference.Model;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.apache.ignite.ml.regressions.logistic.LogisticRegressionModel;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;

/** Parser of Spark models. */
public class SparkModelParser {
    /**
     * Load model from parquet file.
     *
     * @param pathToMdl Hadoop path to model saved from Spark.
     * @param parsedSparkMdl One of supported Spark models to parse it.
     * @return Instance of parsedSparkMdl model.
     */
    public static Model parse(Path pathToMdl, SupportedSparkModels parsedSparkMdl) {

        switch (parsedSparkMdl) {
            case LOG_REGRESSION: return loadLogRegModel(pathToMdl);
            default:
                throw new UnsupportedSparkModelException(parsedSparkMdl.toString());
        }
    }

    /**
     * Load logistic regression model.
     *
     * @param pathToMdl Path to model.
     */
    private static Model loadLogRegModel(Path pathToMdl) {
        Vector coefficients = null;
        double interceptor = 0;

        Configuration conf = new Configuration();
        try {
            ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, pathToMdl, ParquetMetadataConverter.NO_FILTER);
            MessageType schema = readFooter.getFileMetaData().getSchema();

            PageReadStore pages;
            try (ParquetFileReader r = new ParquetFileReader(conf, pathToMdl, readFooter)) {
                final MessageColumnIO colIO = new ColumnIOFactory().getColumnIO(schema);

                while (null != (pages = r.readNextRowGroup())) {
                    final long rows = pages.getRowCount();
                    final RecordReader recordReader = colIO.getRecordReader(pages, new GroupRecordConverter(schema));
                    for (int i = 0; i < rows; i++) {
                        final SimpleGroup g = (SimpleGroup) recordReader.read();
                        interceptor = readInterceptor(g);
                        coefficients = readCoefficients(g);
                    }
                }
            }
        } catch (IOException e) {
            System.out.println("Error reading parquet file.");
            e.printStackTrace();
        }

        return new LogisticRegressionModel(coefficients, interceptor);

    }

    /**
     * Read interceptor value from parquet.
     *
     * @param g Interceptor group.
     */
    private static double readInterceptor(SimpleGroup g) {
        double interceptor;
        final SimpleGroup interceptVector = (SimpleGroup) g.getGroup(2, 0);
        final SimpleGroup interceptVectorVal = (SimpleGroup) interceptVector.getGroup(3, 0);
        final SimpleGroup interceptVectorValElement = (SimpleGroup) interceptVectorVal.getGroup(0, 0);
        interceptor = interceptVectorValElement.getDouble(0, 0);
        return interceptor;
    }

    /**
     * Read coefficient matrix from parquet.
     *
     * @param g Coefficient group.
     * @return Vector of coefficients.
     */
    private static Vector readCoefficients(SimpleGroup g) {
        Vector coefficients;
        final int amountOfCoefficients = g.getGroup(3, 0).getGroup(5, 0).getFieldRepetitionCount(0);

        coefficients = new DenseVector(amountOfCoefficients);

        for (int j = 0; j < amountOfCoefficients; j++) {
            double coefficient = g.getGroup(3, 0).getGroup(5, 0).getGroup(0, j).getDouble(0, 0);
            coefficients.set(j, coefficient);
        }
        return coefficients;
    }
}
