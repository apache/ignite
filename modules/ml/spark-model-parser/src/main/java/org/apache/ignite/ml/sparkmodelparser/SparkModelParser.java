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

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.ml.inference.Model;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.apache.ignite.ml.regressions.linear.LinearRegressionModel;
import org.apache.ignite.ml.regressions.logistic.LogisticRegressionModel;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
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
    public static Model parse(String pathToMdl, SupportedSparkModels parsedSparkMdl) {
        File mdlRsrc = IgniteUtils.resolveIgnitePath(pathToMdl);
        if (mdlRsrc == null)
            throw new IllegalArgumentException("Resource not found [resource_path=" + pathToMdl + "]");

        String ignitePathToMdl = mdlRsrc.getPath();

        switch (parsedSparkMdl) {
            case LOG_REGRESSION:
                return loadLogRegModel(ignitePathToMdl);
            case LINEAR_REGRESSION:
                return loadLinRegModel(ignitePathToMdl);
            default:
                throw new UnsupportedSparkModelException(ignitePathToMdl);
        }
    }

    /**
     * Load linear regression model.
     *
     * @param pathToMdl Path to model.
     */
    private static Model loadLinRegModel(String pathToMdl) {
        Vector coefficients = null;
        double interceptor = 0;

        try (ParquetFileReader r = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(pathToMdl), new Configuration()))) {
            PageReadStore pages;
            final MessageType schema = r.getFooter().getFileMetaData().getSchema();
            final MessageColumnIO colIO = new ColumnIOFactory().getColumnIO(schema);

            while (null != (pages = r.readNextRowGroup())) {
                final long rows = pages.getRowCount();
                final RecordReader recordReader = colIO.getRecordReader(pages, new GroupRecordConverter(schema));
                for (int i = 0; i < rows; i++) {
                    final SimpleGroup g = (SimpleGroup)recordReader.read();
                    interceptor = readInterceptor(g);
                    coefficients = readCoefficients(g);
                }
            }

        }
        catch (IOException e) {
            System.out.println("Error reading parquet file.");
            e.printStackTrace();
        }

        return new LinearRegressionModel(coefficients, interceptor);
    }

    /**
     * Load logistic regression model.
     *
     * @param pathToMdl Path to model.
     */
    private static Model loadLogRegModel(String pathToMdl) {
        Vector coefficients = null;
        double interceptor = 0;

        try (ParquetFileReader r = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(pathToMdl), new Configuration()))) {
            PageReadStore pages;
            final MessageType schema = r.getFooter().getFileMetaData().getSchema();
            final MessageColumnIO colIO = new ColumnIOFactory().getColumnIO(schema);

            while (null != (pages = r.readNextRowGroup())) {
                final long rows = pages.getRowCount();
                final RecordReader recordReader = colIO.getRecordReader(pages, new GroupRecordConverter(schema));
                for (int i = 0; i < rows; i++) {
                    final SimpleGroup g = (SimpleGroup)recordReader.read();
                    interceptor = readInterceptor(g);
                    coefficients = readCoefficients(g);
                }
            }

        }
        catch (IOException e) {
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
        return g.getDouble(0, 0);
    }

    /**
     * Read coefficient matrix from parquet.
     *
     * @param g Coefficient group.
     * @return Vector of coefficients.
     */
    private static Vector readCoefficients(SimpleGroup g) {
        Vector coefficients;
        Group coeffGroup = g.getGroup(1, 0).getGroup(3, 0);

        final int amountOfCoefficients = coeffGroup.getFieldRepetitionCount(0);

        coefficients = new DenseVector(amountOfCoefficients);

        for (int j = 0; j < amountOfCoefficients; j++) {
            double coefficient = coeffGroup.getGroup(0, j).getDouble(0, 0);
            coefficients.set(j, coefficient);
        }
        return coefficients;
    }
}
