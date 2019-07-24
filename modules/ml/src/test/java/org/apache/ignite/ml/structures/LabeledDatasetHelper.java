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

package org.apache.ignite.ml.structures;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.ignite.ml.structures.preprocessing.LabeledDatasetLoader;

/**
 * Base class for decision trees test.
 */
public class LabeledDatasetHelper {
    /** Separator. */
    private static final String SEPARATOR = "\t";

    /**
     * Loads labeled dataset from file with .txt extension.
     *
     * @param rsrcPath path to dataset.
     * @return Null if path is incorrect.
     */
    public static LabeledVectorSet loadDatasetFromTxt(String rsrcPath, boolean isFallOnBadData) {
        try {
            Path path = Paths.get(LabeledDatasetHelper.class.getClassLoader().getResource(rsrcPath).toURI());
            try {
                return LabeledDatasetLoader.loadFromTxtFile(path, SEPARATOR, false, isFallOnBadData);
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
