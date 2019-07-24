/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.apache.ignite.ml.Exporter;
import org.apache.ignite.ml.FileExporter;
import org.apache.ignite.ml.clustering.kmeans.KMeansModel;
import org.apache.ignite.ml.clustering.kmeans.KMeansModelFormat;
import org.apache.ignite.ml.clustering.kmeans.KMeansTrainer;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DoubleArrayVectorizer;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.knn.NNClassificationModel;
import org.apache.ignite.ml.knn.ann.ANNClassificationModel;
import org.apache.ignite.ml.knn.ann.ANNClassificationTrainer;
import org.apache.ignite.ml.knn.ann.ANNModelFormat;
import org.apache.ignite.ml.knn.ann.KNNModelFormat;
import org.apache.ignite.ml.math.distances.ManhattanDistance;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.apache.ignite.ml.regressions.linear.LinearRegressionModel;
import org.apache.ignite.ml.regressions.logistic.LogisticRegressionModel;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.structures.LabeledVectorSet;
import org.apache.ignite.ml.svm.SVMLinearClassificationModel;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for models import/export functionality.
 */
public class LocalModelsTest {
    /**
     *
     */
    @Test
    public void importExportKMeansModelTest() throws IOException {
        executeModelTest(mdlFilePath -> {
            KMeansModel mdl = getClusterModel();

            Exporter<KMeansModelFormat, String> exporter = new FileExporter<>();

            mdl.saveModel(exporter, mdlFilePath);

            KMeansModelFormat load = exporter.load(mdlFilePath);

            assertNotNull(load);

            KMeansModel importedMdl = new KMeansModel(load.getCenters(), load.getDistance());

            assertEquals("", mdl, importedMdl);

            return null;
        });
    }

    /**
     *
     */
    @Test
    public void importExportLinearRegressionModelTest() throws IOException {
        executeModelTest(mdlFilePath -> {
            LinearRegressionModel mdl = new LinearRegressionModel(new DenseVector(new double[] {1, 2}), 3);
            Exporter<LinearRegressionModel, String> exporter = new FileExporter<>();
            mdl.saveModel(exporter, mdlFilePath);

            LinearRegressionModel load = exporter.load(mdlFilePath);

            assertNotNull(load);
            assertEquals("", mdl, load);

            return null;
        });
    }

    /**
     *
     */
    @Test
    public void importExportSVMBinaryClassificationModelTest() throws IOException {
        executeModelTest(mdlFilePath -> {
            SVMLinearClassificationModel mdl = new SVMLinearClassificationModel(new DenseVector(new double[] {1, 2}), 3);
            Exporter<SVMLinearClassificationModel, String> exporter = new FileExporter<>();
            mdl.saveModel(exporter, mdlFilePath);

            SVMLinearClassificationModel load = exporter.load(mdlFilePath);

            assertNotNull(load);
            assertEquals("", mdl, load);

            return null;
        });
    }

    /**
     *
     */
    @Test
    public void importExportLogisticRegressionModelTest() throws IOException {
        executeModelTest(mdlFilePath -> {
            LogisticRegressionModel mdl = new LogisticRegressionModel(new DenseVector(new double[] {1, 2}), 3);
            Exporter<LogisticRegressionModel, String> exporter = new FileExporter<>();
            mdl.saveModel(exporter, mdlFilePath);

            LogisticRegressionModel load = exporter.load(mdlFilePath);

            assertNotNull(load);
            assertEquals("", mdl, load);

            return null;
        });
    }

    /**
     *
     */
    private void executeModelTest(Function<String, Void> code) throws IOException {
        Path mdlPath = Files.createTempFile(null, null);

        assertNotNull(mdlPath);

        try {
            String mdlFilePath = mdlPath.toAbsolutePath().toString();

            assertTrue(String.format("File %s not found.", mdlFilePath), Files.exists(mdlPath));

            code.apply(mdlFilePath);
        }
        finally {
            Files.deleteIfExists(mdlPath);
        }
    }

    /**
     *
     */
    private KMeansModel getClusterModel() {
        Map<Integer, double[]> data = new HashMap<>();
        data.put(0, new double[] {1.0, 1959, 325100});
        data.put(1, new double[] {1.0, 1960, 373200});

        KMeansTrainer trainer = new KMeansTrainer()
            .withAmountOfClusters(1);

        return trainer.fit(
            new LocalDatasetBuilder<>(data, 2),
            new DoubleArrayVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.LAST)
        );
    }

    /**
     *
     */
    @Test
    public void importExportANNModelTest() throws IOException {
        executeModelTest(mdlFilePath -> {
            final LabeledVectorSet<LabeledVector> centers = new LabeledVectorSet<>();

            NNClassificationModel mdl = new ANNClassificationModel(centers, new ANNClassificationTrainer.CentroidStat())
                .withK(4)
                .withDistanceMeasure(new ManhattanDistance())
                .withWeighted(true);

            Exporter<KNNModelFormat, String> exporter = new FileExporter<>();
            mdl.saveModel(exporter, mdlFilePath);

            ANNModelFormat load = (ANNModelFormat)exporter.load(mdlFilePath);

            assertNotNull(load);

            NNClassificationModel importedMdl = new ANNClassificationModel(load.getCandidates(),
                new ANNClassificationTrainer.CentroidStat())
                .withK(load.getK())
                .withDistanceMeasure(load.getDistanceMeasure())
                .withWeighted(load.isWeighted());

            assertEquals("", mdl, importedMdl);

            return null;
        });
    }
}
