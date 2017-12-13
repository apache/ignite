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
import org.apache.ignite.Ignite;
import org.apache.ignite.ml.structures.LabeledDataset;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Base class for decision trees test.
 */
public class BaseKNNTest extends GridCommonAbstractTest {
    /** Count of nodes. */
    private static final int NODE_COUNT = 4;

    /** Separator. */
    private static final String SEPARATOR = "\t";

    /** Path to the Iris dataset. */
    static final String KNN_IRIS_TXT = "datasets/knn/iris.txt";

    /** Grid instance. */
    protected Ignite ignite;

    /**
     * Default constructor.
     */
    public BaseKNNTest() {
        super(false);
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        ignite = grid(NODE_COUNT);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        for (int i = 1; i <= NODE_COUNT; i++)
            startGrid(i);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * Loads labeled dataset from file with .txt extension.
     *
     * @param rsrcPath path to dataset.
     * @return null if path is incorrect.
     */
    LabeledDataset loadDatasetFromTxt(String rsrcPath, boolean isFallOnBadData) {
        try {
            Path path = Paths.get(this.getClass().getClassLoader().getResource(rsrcPath).toURI());
            try {
                return LabeledDataset.loadTxt(path, SEPARATOR, false, isFallOnBadData);
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
        catch (URISyntaxException e) {
            e.printStackTrace();
            return null;
        }
        return null;
    }
}
