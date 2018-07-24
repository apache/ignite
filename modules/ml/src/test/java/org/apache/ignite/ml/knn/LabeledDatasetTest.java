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

package org.apache.ignite.ml.knn;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.ignite.ml.math.ExternalizableTest;
import org.apache.ignite.ml.math.exceptions.CardinalityException;
import org.apache.ignite.ml.math.exceptions.NoDataException;
import org.apache.ignite.ml.math.exceptions.knn.EmptyFileException;
import org.apache.ignite.ml.math.exceptions.knn.FileParsingException;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.structures.LabeledDataset;
import org.apache.ignite.ml.structures.LabeledDatasetTestTrainPair;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.structures.preprocessing.LabeledDatasetLoader;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.fail;

/** Tests behaviour of KNNClassificationTest. */
public class LabeledDatasetTest implements ExternalizableTest<LabeledDataset> {
    /** */
    private static final String KNN_IRIS_TXT = "datasets/knn/iris.txt";

    /** */
    private static final String NO_DATA_TXT = "datasets/knn/no_data.txt";

    /** */
    private static final String EMPTY_TXT = "datasets/knn/empty.txt";

    /** */
    private static final String IRIS_INCORRECT_TXT = "datasets/knn/iris_incorrect.txt";

    /** */
    private static final String IRIS_MISSED_DATA = "datasets/knn/missed_data.txt";

    /** */
    @Test
    public void testFeatureNames() {
        double[][] mtx =
            new double[][] {
                {1.0, 1.0},
                {1.0, 2.0},
                {2.0, 1.0},
                {-1.0, -1.0},
                {-1.0, -2.0},
                {-2.0, -1.0}};
        double[] lbs = new double[] {1.0, 1.0, 1.0, 2.0, 2.0, 2.0};

        String[] featureNames = new String[] {"x", "y"};
        final LabeledDataset dataset = new LabeledDataset(mtx, lbs, featureNames, false);

        assertEquals(dataset.getFeatureName(0), "x");
    }

    /** */
    @Test
    public void testAccessMethods() {
        double[][] mtx =
            new double[][] {
                {1.0, 1.0},
                {1.0, 2.0},
                {2.0, 1.0},
                {-1.0, -1.0},
                {-1.0, -2.0},
                {-2.0, -1.0}};
        double[] lbs = new double[] {1.0, 1.0, 1.0, 2.0, 2.0, 2.0};

        final LabeledDataset dataset = new LabeledDataset(mtx, lbs, null, false);

        assertEquals(dataset.colSize(), 2);
        assertEquals(dataset.rowSize(), 6);

        final LabeledVector<Vector, Double> row = (LabeledVector<Vector, Double>)dataset.getRow(0);

        assertEquals(row.features().get(0), 1.0);
        assertEquals(row.label(), 1.0);
        dataset.setLabel(0, 2.0);
        assertEquals(row.label(), 2.0);
    }

    /** */
    @Test
    public void testFailOnYNull() {
        double[][] mtx =
            new double[][] {
                {1.0, 1.0},
                {1.0, 2.0},
                {2.0, 1.0},
                {-1.0, -1.0},
                {-1.0, -2.0},
                {-2.0, -1.0}};
        double[] lbs = new double[] {};

        try {
            new LabeledDataset(mtx, lbs);
            fail("CardinalityException");
        }
        catch (CardinalityException e) {
            return;
        }
        fail("CardinalityException");
    }

    /** */
    @Test
    public void testFailOnXNull() {
        double[][] mtx =
            new double[][] {};
        double[] lbs = new double[] {1.0, 1.0, 1.0, 2.0, 2.0, 2.0};

        try {
            new LabeledDataset(mtx, lbs);
            fail("CardinalityException");
        }
        catch (CardinalityException e) {
            return;
        }
        fail("CardinalityException");
    }

    /** */
    @Test
    public void testLoadingCorrectTxtFile() {
        LabeledDataset training = LabeledDatasetHelper.loadDatasetFromTxt(KNN_IRIS_TXT, false);
        assertEquals(training.rowSize(), 150);
    }

    /** */
    @Test
    public void testLoadingEmptyFile() {
        try {
            LabeledDatasetHelper.loadDatasetFromTxt(EMPTY_TXT, false);
            fail("EmptyFileException");
        }
        catch (EmptyFileException e) {
            return;
        }
        fail("EmptyFileException");
    }

    /** */
    @Test
    public void testLoadingFileWithFirstEmptyRow() {
        try {
            LabeledDatasetHelper.loadDatasetFromTxt(NO_DATA_TXT, false);
            fail("NoDataException");
        }
        catch (NoDataException e) {
            return;
        }
        fail("NoDataException");
    }

    /** */
    @Test
    public void testLoadingFileWithIncorrectData() {
        LabeledDataset training = LabeledDatasetHelper.loadDatasetFromTxt(IRIS_INCORRECT_TXT, false);
        assertEquals(149, training.rowSize());
    }

    /** */
    @Test
    public void testFailOnLoadingFileWithIncorrectData() {
        try {
            LabeledDatasetHelper.loadDatasetFromTxt(IRIS_INCORRECT_TXT, true);
            fail("FileParsingException");
        }
        catch (FileParsingException e) {
            return;
        }
        fail("FileParsingException");

    }

    /** */
    @Test
    public void testLoadingFileWithMissedData() throws URISyntaxException, IOException {
        Path path = Paths.get(this.getClass().getClassLoader().getResource(IRIS_MISSED_DATA).toURI());

        LabeledDataset training = LabeledDatasetLoader.loadFromTxtFile(path, ",", false, false);

        assertEquals(training.features(2).get(1), 0.0);
    }

    /** */
    @Test
    public void testSplitting() {
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

        LabeledDatasetTestTrainPair split1 = new LabeledDatasetTestTrainPair(training, 0.67);

        assertEquals(4, split1.test().rowSize());
        assertEquals(2, split1.train().rowSize());

        LabeledDatasetTestTrainPair split2 = new LabeledDatasetTestTrainPair(training, 0.65);

        assertEquals(3, split2.test().rowSize());
        assertEquals(3, split2.train().rowSize());

        LabeledDatasetTestTrainPair split3 = new LabeledDatasetTestTrainPair(training, 0.4);

        assertEquals(2, split3.test().rowSize());
        assertEquals(4, split3.train().rowSize());

        LabeledDatasetTestTrainPair split4 = new LabeledDatasetTestTrainPair(training, 0.3);

        assertEquals(1, split4.test().rowSize());
        assertEquals(5, split4.train().rowSize());
    }

    /** */
    @Test
    public void testLabels() {
        double[][] mtx =
            new double[][] {
                {1.0, 1.0},
                {1.0, 2.0},
                {2.0, 1.0},
                {-1.0, -1.0},
                {-1.0, -2.0},
                {-2.0, -1.0}};
        double[] lbs = new double[] {1.0, 1.0, 1.0, 2.0, 2.0, 2.0};

        LabeledDataset dataset = new LabeledDataset(mtx, lbs);
        final double[] labels = dataset.labels();
        for (int i = 0; i < lbs.length; i++)
            assertEquals(lbs[i], labels[i]);
    }

    /** */
    @Override public void testExternalization() {
        double[][] mtx =
            new double[][] {
                {1.0, 1.0},
                {1.0, 2.0},
                {2.0, 1.0},
                {-1.0, -1.0},
                {-1.0, -2.0},
                {-2.0, -1.0}};
        double[] lbs = new double[] {1.0, 1.0, 1.0, 2.0, 2.0, 2.0};

        LabeledDataset dataset = new LabeledDataset(mtx, lbs);
        this.externalizeTest(dataset);
    }
}
