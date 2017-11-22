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
import java.nio.file.Paths;
import org.apache.ignite.ml.clustering.KMeansLocalClusterer;
import org.apache.ignite.ml.clustering.KMeansModel;
import org.apache.ignite.ml.math.EuclideanDistance;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for models import/export functionality.
 */
public class LocalModelsTest {
    /** */
    private String mdlFilePath = "model.mlmod";

    /**
     *
     */
    @After
    public void cleanUp() throws IOException {
        Files.deleteIfExists(Paths.get(mdlFilePath));
    }

    /**
     *
     */
    @Test
    public void importExportKMeansModelTest(){
        Path mdlPath = Paths.get(mdlFilePath);

        KMeansModel mdl = getClusterModel();

        Exporter<KMeansModelFormat, String> exporter = new FileExporter<>();
        mdl.saveModel(exporter, mdlFilePath);

        Assert.assertTrue(String.format("File %s not found.", mdlPath.toString()), Files.exists(mdlPath));

        KMeansModelFormat load = exporter.load(mdlFilePath);
        KMeansModel importedMdl = new KMeansModel(load.getCenters(), load.getDistance());

        Assert.assertTrue("", mdl.equals(importedMdl));
    }

    /**
     *
     */
    private KMeansModel getClusterModel(){
        KMeansLocalClusterer clusterer = new KMeansLocalClusterer(new EuclideanDistance(), 1, 1L);

        double[] v1 = new double[] {1959, 325100};
        double[] v2 = new double[] {1960, 373200};

        DenseLocalOnHeapMatrix points = new DenseLocalOnHeapMatrix(new double[][] {v1, v2});

        return clusterer.cluster(points, 1);
    }
}
