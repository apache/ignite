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

package org.apache.ignite.ml.common;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.apache.ignite.ml.Exporter;
import org.apache.ignite.ml.FileExporter;
import org.apache.ignite.ml.clustering.kmeans.KMeansModel;
import org.apache.ignite.ml.clustering.kmeans.KMeansModelFormat;
import org.apache.ignite.ml.clustering.kmeans.KMeansTrainer;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.knn.NNClassificationModel;
import org.apache.ignite.ml.knn.ann.ANNClassificationModel;
import org.apache.ignite.ml.knn.ann.ANNClassificationTrainer;
import org.apache.ignite.ml.knn.ann.ANNModelFormat;
import org.apache.ignite.ml.knn.ann.ProbableLabel;
import org.apache.ignite.ml.knn.classification.KNNClassificationModel;
import org.apache.ignite.ml.knn.classification.KNNModelFormat;
import org.apache.ignite.ml.knn.classification.NNStrategy;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.math.distances.ManhattanDistance;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.apache.ignite.ml.regressions.linear.LinearRegressionModel;
import org.apache.ignite.ml.regressions.logistic.binomial.LogisticRegressionModel;
import org.apache.ignite.ml.regressions.logistic.multiclass.LogRegressionMultiClassModel;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.structures.LabeledVectorSet;
import org.apache.ignite.ml.svm.SVMLinearBinaryClassificationModel;
import org.apache.ignite.ml.svm.SVMLinearMultiClassClassificationModel;
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

            Assert.assertEquals("", mdl, importedMdl);

            return null;
        });
    }

    /** */
    @Test
    public void importExportLinearRegressionModelTest() throws IOException {
        executeModelTest(mdlFilePath -> {
            LinearRegressionModel mdl = new LinearRegressionModel(new DenseVector(new double[]{1, 2}), 3);
            Exporter<LinearRegressionModel, String> exporter = new FileExporter<>();
            mdl.saveModel(exporter, mdlFilePath);

            LinearRegressionModel load = exporter.load(mdlFilePath);

            Assert.assertNotNull(load);
            Assert.assertEquals("", mdl, load);

            return null;
        });
    }

    /** */
    @Test
    public void importExportSVMBinaryClassificationModelTest() throws IOException {
        executeModelTest(mdlFilePath -> {
            SVMLinearBinaryClassificationModel mdl = new SVMLinearBinaryClassificationModel(new DenseVector(new double[]{1, 2}), 3);
            Exporter<SVMLinearBinaryClassificationModel, String> exporter = new FileExporter<>();
            mdl.saveModel(exporter, mdlFilePath);

            SVMLinearBinaryClassificationModel load = exporter.load(mdlFilePath);

            Assert.assertNotNull(load);
            Assert.assertEquals("", mdl, load);

            return null;
        });
    }

    /** */
    @Test
    public void importExportSVMMultiClassClassificationModelTest() throws IOException {
        executeModelTest(mdlFilePath -> {
            SVMLinearBinaryClassificationModel binaryMdl1 = new SVMLinearBinaryClassificationModel(new DenseVector(new double[]{1, 2}), 3);
            SVMLinearBinaryClassificationModel binaryMdl2 = new SVMLinearBinaryClassificationModel(new DenseVector(new double[]{2, 3}), 4);
            SVMLinearBinaryClassificationModel binaryMdl3 = new SVMLinearBinaryClassificationModel(new DenseVector(new double[]{3, 4}), 5);

            SVMLinearMultiClassClassificationModel mdl = new SVMLinearMultiClassClassificationModel();
            mdl.add(1, binaryMdl1);
            mdl.add(2, binaryMdl2);
            mdl.add(3, binaryMdl3);

            Exporter<SVMLinearMultiClassClassificationModel, String> exporter = new FileExporter<>();
            mdl.saveModel(exporter, mdlFilePath);

            SVMLinearMultiClassClassificationModel load = exporter.load(mdlFilePath);

            Assert.assertNotNull(load);
            Assert.assertEquals("", mdl, load);

            return null;
        });
    }

    /** */
    @Test
    public void importExportLogisticRegressionModelTest() throws IOException {
        executeModelTest(mdlFilePath -> {
            LogisticRegressionModel mdl = new LogisticRegressionModel(new DenseVector(new double[]{1, 2}), 3);
            Exporter<LogisticRegressionModel, String> exporter = new FileExporter<>();
            mdl.saveModel(exporter, mdlFilePath);

            LogisticRegressionModel load = exporter.load(mdlFilePath);

            Assert.assertNotNull(load);
            Assert.assertEquals("", mdl, load);

            return null;
        });
    }

    /** */
    @Test
    public void importExportLogRegressionMultiClassModelTest() throws IOException {
        executeModelTest(mdlFilePath -> {
            LogRegressionMultiClassModel mdl = new LogRegressionMultiClassModel();
            Exporter<LogRegressionMultiClassModel, String> exporter = new FileExporter<>();
            mdl.saveModel(exporter, mdlFilePath);

            LogRegressionMultiClassModel load = exporter.load(mdlFilePath);

            Assert.assertNotNull(load);
            Assert.assertEquals("", mdl, load);

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
        Map<Integer, double[]> data = new HashMap<>();
        data.put(0, new double[] {1.0, 1959, 325100});
        data.put(1, new double[] {1.0, 1960, 373200});

        KMeansTrainer trainer = new KMeansTrainer()
            .withAmountOfClusters(1);

        return trainer.fit(
            new LocalDatasetBuilder<>(data, 2),
            (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 0, v.length - 1)),
            (k, v) -> v[2]
        );
    }

    /** */
    @Test
    public void importExportKNNModelTest() throws IOException {
        executeModelTest(mdlFilePath -> {
            NNClassificationModel mdl = new KNNClassificationModel(null)
                .withK(3)
                .withDistanceMeasure(new EuclideanDistance())
                .withStrategy(NNStrategy.SIMPLE);

            Exporter<KNNModelFormat, String> exporter = new FileExporter<>();
            mdl.saveModel(exporter, mdlFilePath);

            KNNModelFormat load = exporter.load(mdlFilePath);

            Assert.assertNotNull(load);

            NNClassificationModel importedMdl = new KNNClassificationModel(null)
                .withK(load.getK())
                .withDistanceMeasure(load.getDistanceMeasure())
                .withStrategy(load.getStgy());

            Assert.assertEquals("", mdl, importedMdl);

            return null;
        });
    }

    /** */
    @Test
    public void importExportANNModelTest() throws IOException {
        executeModelTest(mdlFilePath -> {
            final LabeledVectorSet<ProbableLabel, LabeledVector> centers = new LabeledVectorSet<>();

            NNClassificationModel mdl = new ANNClassificationModel(centers, new ANNClassificationTrainer.CentroidStat())
                .withK(4)
                .withDistanceMeasure(new ManhattanDistance())
                .withStrategy(NNStrategy.WEIGHTED);

            Exporter<KNNModelFormat, String> exporter = new FileExporter<>();
            mdl.saveModel(exporter, mdlFilePath);

            ANNModelFormat load = (ANNModelFormat) exporter.load(mdlFilePath);

            Assert.assertNotNull(load);


            NNClassificationModel importedMdl = new ANNClassificationModel(load.getCandidates(), new ANNClassificationTrainer.CentroidStat())
                .withK(load.getK())
                .withDistanceMeasure(load.getDistanceMeasure())
                .withStrategy(load.getStgy());

            Assert.assertEquals("", mdl, importedMdl);

            return null;
        });
    }
}
