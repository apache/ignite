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
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.ml.knn.models.FillMissingValueWith;
import org.apache.ignite.ml.math.exceptions.NoDataException;
import org.apache.ignite.ml.math.exceptions.knn.EmptyFileException;
import org.apache.ignite.ml.math.exceptions.knn.FileParsingException;
import org.apache.ignite.ml.structures.LabeledDataset;

/** Tests behaviour of KNNClassificationTest. */
public class LabeledDatasetTest extends BaseKNNTest {

    private static final String KNN_IRIS_TXT = "knn/iris.txt";
    private static final String NO_DATA_TXT = "knn/no_data.txt";
    private static final String EMPTY_TXT = "knn/empty.txt";
    private static final String IRIS_INCORRECT_TXT = "knn/iris_incorrect.txt";
    private static final String IRIS_MISSED_DATA = "knn/missed_data.txt";

    /** */
    public void testLoadingCorrectTxtFile() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());
        LabeledDataset training = loadIrisDataset(KNN_IRIS_TXT, false);
        assertEquals(training.rowSize(), 150);
    }

    /** */
    public void testLoadingEmptyFile() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        try {
            LabeledDataset training = loadIrisDataset(EMPTY_TXT, false);
            fail("EmptyFileException");
        }
        catch (EmptyFileException e) {
            return;
        }
        fail("EmptyFileException");
    }

    /** */
    public void testLoadingFileWithFirstEmptyRow() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        try {
            LabeledDataset training = loadIrisDataset(NO_DATA_TXT, false);
            fail("NoDataException");
        }
        catch (NoDataException e) {
            return;
        }
        fail("NoDataException");
    }

    /** */
    public void testLoadingFileWithIncorrectData() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        LabeledDataset training = loadIrisDataset(IRIS_INCORRECT_TXT, false);
        assertEquals(149, training.rowSize());
    }

    /** */
    public void testFailOnLoadingFileWithIncorrectData() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        try {
            LabeledDataset training = loadIrisDataset(IRIS_INCORRECT_TXT, true);
            fail("FileParsingException");
        }
        catch (FileParsingException e) {
            return;
        }
        fail("FileParsingException");

    }

    /** */
    public void testLoadingFileWithMissedData() throws URISyntaxException, IOException {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        Path path = Paths.get(this.getClass().getClassLoader().getResource(IRIS_MISSED_DATA).toURI());

        LabeledDataset training = LabeledDataset.loadTxt(path, ",", false, false, FillMissingValueWith.ZERO);

        assertEquals(training.features(2).get(1), 0.0);

    }
}
