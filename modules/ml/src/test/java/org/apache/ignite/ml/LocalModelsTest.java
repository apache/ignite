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

package org.apache.ignite.ml;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Function;
import org.apache.ignite.ml.clustering.KMeansLocalClusterer;
import org.apache.ignite.ml.clustering.KMeansModel;
import org.apache.ignite.ml.knn.models.KNNModel;
import org.apache.ignite.ml.knn.models.KNNModelFormat;
import org.apache.ignite.ml.knn.models.KNNStrategy;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.ml.regressions.linear.LinearRegressionModel;
import org.apache.ignite.ml.structures.LabeledDataset;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for models import/export functionality.
 */
public class LocalModelsTest {
    /** */
    @Test
    public void importExportKMeansModelTest() throws IOException {
        executeModelTest(mdlFilePath -> {
            KMeansModel mdl = getClusterModel();

            Exporter<KMeansModelFormat, String> exporter = new FileExporter<>();

            mdl.saveModel(exporter, mdlFilePath);

            KMeansModelFormat load = exporter.load(mdlFilePath);

            Assert.assertNotNull(load);

            KMeansModel importedMdl = new KMeansModel(load.getCenters(), load.getDistance());

            Assert.assertTrue("", mdl.equals(importedMdl));

            return null;
        });
    }

    /** */
    @Test
    public void importExportLinearRegressionModelTest() throws IOException {
        executeModelTest(mdlFilePath -> {
            LinearRegressionModel model = new LinearRegressionModel(new DenseLocalOnHeapVector(new double[]{1, 2}), 3);
            Exporter<LinearRegressionModel, String> exporter = new FileExporter<>();
            model.saveModel(exporter, mdlFilePath);

            LinearRegressionModel load = exporter.load(mdlFilePath);

            Assert.assertNotNull(load);
            Assert.assertEquals("", model, load);

            return null;
        });
    }

    /** */
    private void executeModelTest(Function<String, Void> code) throws IOException {
        Path mdlPath = Files.createTempFile(null, null);

        Assert.assertNotNull(mdlPath);

        try {
            String mdlFilePath = mdlPath.toAbsolutePath().toString();

            Assert.assertTrue(String.format("File %s not found.", mdlFilePath), Files.exists(mdlPath));

            code.apply(mdlFilePath);
        }
        finally {
            Files.deleteIfExists(mdlPath);
        }
    }

    /** */
    private KMeansModel getClusterModel() {
        KMeansLocalClusterer clusterer = new KMeansLocalClusterer(new EuclideanDistance(), 1, 1L);

        double[] v1 = new double[] {1959, 325100};
        double[] v2 = new double[] {1960, 373200};

        DenseLocalOnHeapMatrix points = new DenseLocalOnHeapMatrix(new double[][] {v1, v2});

        return clusterer.cluster(points, 1);
    }

    /** */
    @Test
    public void importExportKNNModelTest() throws IOException {
        executeModelTest(mdlFilePath -> {
            double[][] mtx =
                new double[][] {
                    {1.0, 1.0},
                    {1.0, 2.0},
                    {2.0, 1.0},
                    {-1.0, -1.0},
                    {-1.0, -2.0},
                    {-2.0, -1.0}};
            double[] lbs = new double[] {1.0, 1.0, 1.0, 2.0, 2.0, 2.0};

            LabeledDataset training = new LabeledDataset(mtx, lbs);

            KNNModel mdl = new KNNModel(3, new EuclideanDistance(), KNNStrategy.SIMPLE, training);

            Exporter<KNNModelFormat, String> exporter = new FileExporter<>();
            mdl.saveModel(exporter, mdlFilePath);

            KNNModelFormat load = exporter.load(mdlFilePath);

            Assert.assertNotNull(load);

            KNNModel importedMdl = new KNNModel(load.getK(), load.getDistanceMeasure(), load.getStgy(), load.getTraining());

            Assert.assertTrue("", mdl.equals(importedMdl));

            return null;
        });
    }
}
